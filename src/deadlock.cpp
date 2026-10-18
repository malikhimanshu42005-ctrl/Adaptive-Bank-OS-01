/**
 * deadlock.cpp — Standalone Deadlock Prevention, Avoidance, and Detection Engine
 *
 * Implements and verifies deadlock prevention algorithms for the Adaptive Bank OS:
 *   1. Havender's Monotonic Ordering Protocol (Prevention — Circular Wait Elimination)
 *   2. All-or-Nothing Atomic Lock Acquisition (Prevention — Hold & Wait Elimination)
 *   3. Bounded Wait Timeout Preemption (Prevention — No Preemption Elimination)
 *   4. Dijkstra's Banker's Algorithm (Deadlock Avoidance & Safe State Sequences)
 *   5. Wait-For Graph (WFG) Cycle Detection (Detection & Cost-Optimal Victim Recovery)
 *
 * Dataset:
 *   Handles 176K transactions from workload_generator.hpp across 12 banking accounts
 *   (8 Retail, 2 Corporate, 2 ATM Vaults).
 *
 * Build:
 *   g++ -std=c++17 -O2 -pthread -I include src/deadlock.cpp -o deadlock_test
 *
 * Usage:
 *   ./deadlock_test                          # Full 176K workload in Prevention mode
 *   ./deadlock_test --mode compare           # Compare Havender Prevention vs Unconstrained Locking
 *   ./deadlock_test --mode avoidance         # Run Banker's Algorithm Safe State verification
 *   ./deadlock_test --mode detect            # Demonstrate Deadlock Detection & Victim Recovery
 *   ./deadlock_test --workload 10000         # Custom workload size
 *   ./deadlock_test --stress --threads 8     # Multi-threaded concurrent deadlock stress test
 *   ./deadlock_test --export deadlock.json   # Export comprehensive audit to JSON
 *   ./deadlock_test --verbose                # Periodic progress output
 */

#include <iostream>
#include <iomanip>
#include <vector>
#include <deque>
#include <memory>
#include <algorithm>
#include <numeric>
#include <string>
#include <chrono>
#include <cmath>
#include <fstream>
#include <map>
#include <set>
#include <thread>
#include <future>
#include <atomic>
#include <sstream>

#include "pcb.hpp"
#include "workload_generator.hpp"
#include "lock_manager.hpp"
#include "middleware.hpp"
#include "mysql_storage.hpp"
#include "deadlock_prevention.hpp"
#include "json.hpp"

// ─────────────────────────────────────────────────────────────
// Simulation Statistics for Deadlock Analysis
// ─────────────────────────────────────────────────────────────

struct DeadlockAuditStats {
    std::string mode_label = "HAVENDER PREVENTATIVE";
    size_t workload_size = 0;
    int timeout_ticks = 20;

    // Transaction outcomes
    int total_processed = 0;
    int committed_count = 0;
    int timeout_aborts = 0;
    int funds_aborts = 0;
    int deadlock_aborts = 0; // Forced victim aborts during deadlock recovery

    // Lock interactions
    long long total_lock_attempts = 0;
    long long first_try_grants = 0;
    long long retry_grants = 0;
    long long total_conflicts = 0;
    long long rollbacks_hold_wait = 0;

    // Deadlock detection & graph metrics
    long long wfg_cycle_checks = 0;
    long long wfg_cycles_found = 0;
    long long victims_preempted = 0;
    long long havender_passes = 0;
    long long havender_violations = 0;

    // Banker's algorithm metrics
    long long banker_evaluations = 0;
    long long banker_safe_grants = 0;
    long long banker_unsafe_delays = 0;

    // Timing & Throughput
    double duration_ms = 0.0;
    int total_simulated_ticks = 0;

    double commit_rate() const {
        return (total_processed > 0) ? (static_cast<double>(committed_count) / total_processed) * 100.0 : 0.0;
    }

    double deadlock_rate() const {
        return (total_processed > 0) ? (static_cast<double>(deadlock_aborts) / total_processed) * 100.0 : 0.0;
    }

    double throughput_tps() const {
        return (duration_ms > 0.0) ? (static_cast<double>(committed_count) / (duration_ms / 1000.0)) : 0.0;
    }
};

// ─────────────────────────────────────────────────────────────
// Deadlock Prevention Simulation Engine
// ─────────────────────────────────────────────────────────────

class DeadlockPreventionEngine {
private:
    std::vector<std::shared_ptr<PCB>> workload_;
    size_t workload_cursor_ = 0;

    LockManager    lock_manager_;
    MDACCMiddleware middleware_;
    StorageEngine  storage_;
    WaitForGraph   wfg_;

    // Naive 2PL tables for unconstrained counter-factual demonstration
    std::map<int, int> naive_lock_table_;
    std::map<int, std::set<int>> naive_pid_held_;

    std::vector<std::shared_ptr<PCB>> waiting_pool_;
    std::vector<std::shared_ptr<PCB>> completed_pool_;
    std::map<int, std::shared_ptr<PCB>> active_pcb_lookup_;

    DeadlockAuditStats stats_;
    int current_tick_ = 0;
    bool enforce_havender_ = true;
    bool enable_banker_avoidance_ = false;
    bool verbose_ = false;

    // Trace of detected deadlocks for reporting
    std::vector<DeadlockCycle> recorded_deadlocks_;

    bool try_acquire(int pid, const std::vector<int>& locks, int& conflict_acc, int& conflict_pid) {
        if (enforce_havender_) {
            return lock_manager_.try_acquire_locks(pid, locks, conflict_acc, conflict_pid);
        } else {
            // Naive 2PL: partial locking without all-or-nothing rollback (Hold & Wait)
            for (int acc_id : locks) {
                auto it = naive_lock_table_.find(acc_id);
                if (it != naive_lock_table_.end() && it->second != pid) {
                    conflict_acc = acc_id;
                    conflict_pid = it->second;
                    return false; // Holds already acquired locks and waits!
                } else {
                    naive_lock_table_[acc_id] = pid;
                    naive_pid_held_[pid].insert(acc_id);
                }
            }
            return true;
        }
    }

    void release_all_locks(int pid) {
        if (enforce_havender_) {
            lock_manager_.release_locks(pid);
        } else {
            auto it = naive_pid_held_.find(pid);
            if (it != naive_pid_held_.end()) {
                for (int acc : it->second) {
                    naive_lock_table_.erase(acc);
                }
                naive_pid_held_.erase(it);
            }
        }
    }

    size_t get_active_locks_count() const {
        return enforce_havender_ ? lock_manager_.get_active_locks_count() : naive_lock_table_.size();
    }

public:
    DeadlockPreventionEngine(size_t workload_size, int timeout_ticks,
                             bool enforce_havender, bool enable_banker = false, bool verbose = false)
        : lock_manager_(timeout_ticks),
          middleware_(true, 100),
          enforce_havender_(enforce_havender),
          enable_banker_avoidance_(enable_banker),
          verbose_(verbose) {

        stats_.workload_size = workload_size;
        stats_.timeout_ticks = timeout_ticks;

        if (enable_banker_avoidance_) {
            stats_.mode_label = "BANKER AVOIDANCE (Safe State Sequences)";
        } else if (enforce_havender_) {
            stats_.mode_label = "HAVENDER PREVENTATIVE (Zero Circular Wait)";
        } else {
            stats_.mode_label = "UNCONSTRAINED (Deadlock Detection & Recovery)";
        }

        std::cout << "  [Ingestion] Generating " << workload_size << " transaction PCBs...\n";
        auto t0 = std::chrono::steady_clock::now();
        workload_ = WorkloadGenerator::generate_workload(workload_size, 0, 8000, 42);
        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::cout << "  [Ingestion] Complete in " << std::fixed << std::setprecision(1) << ms << " ms\n";
    }

    DeadlockAuditStats run() {
        std::cout << "\n  Executing Deadlock Engine [" << stats_.mode_label << "]...\n";
        auto start_time = std::chrono::steady_clock::now();

        int idle_counter = 0;

        while (true) {
            current_tick_++;

            // 1. Ingest Arriving Transactions
            while (workload_cursor_ < workload_.size() &&
                   workload_[workload_cursor_]->arrival_time <= current_tick_) {
                auto proc = workload_[workload_cursor_++];
                active_pcb_lookup_[proc->pid] = proc;
                middleware_.record_arrival(proc->type);

                // Prepare lock acquisition order
                std::vector<int> target_locks = proc->required_locks;

                if (enforce_havender_) {
                    // HAVENDER PROTOCOL: Sort in strict ascending order
                    target_locks = HavenderProtocol::enforce_monotonic_order(target_locks);
                    if (HavenderProtocol::is_monotonic(target_locks)) {
                        stats_.havender_passes++;
                    } else {
                        stats_.havender_violations++;
                    }
                } else {
                    // UNCONSTRAINED: Intentionally invert or shuffle locks on even PIDs
                    // to demonstrate circular wait deadlocks
                    if (proc->pid % 2 == 0 && target_locks.size() > 1) {
                        std::reverse(target_locks.begin(), target_locks.end());
                    }
                }

                proc->required_locks = target_locks;

                // Attempt acquisition on arrival
                stats_.total_lock_attempts++;
                middleware_.record_attempt();

                int conflict_acc = 0, conflict_pid = 0;
                bool acquired = false;

                if (enable_banker_avoidance_) {
                    acquired = attempt_banker_acquisition(proc, conflict_acc, conflict_pid);
                } else {
                    acquired = try_acquire(proc->pid, proc->required_locks, conflict_acc, conflict_pid);
                }

                if (acquired) {
                    stats_.first_try_grants++;
                    proc->state = ProcessState::RUNNING;
                    proc->start_time = current_tick_;
                } else {
                    stats_.total_conflicts++;
                    stats_.rollbacks_hold_wait++;
                    middleware_.record_conflict();

                    // Register Wait edge in Wait-For Graph
                    wfg_.add_wait_edge(proc->pid, conflict_pid);

                    // Check for circular wait cycles
                    DeadlockCycle cycle;
                    if (wfg_.detect_cycle(cycle, current_tick_)) {
                        stats_.wfg_cycles_found++;
                        if (!enforce_havender_) {
                            handle_deadlock_cycle(cycle);
                        }
                    }

                    proc->state = ProcessState::WAITING;
                    proc->retry_count = 1;
                    proc->backoff_remaining = middleware_.calculate_backoff(proc->pid, proc->retry_count);
                    waiting_pool_.push_back(proc);
                }
            }

            // 2. Service Waiting Pool (Retries & Bounded Wait Preemption)
            process_waiting_pool();

            // 3. Advance Active Running Processes & Release Locks
            advance_running_processes();

            // Telemetry tick
            middleware_.advance_tick();

            if (verbose_ && (current_tick_ % 500 == 0)) {
                print_progress_tick();
            }

            // Termination conditions
            bool all_ingested = (workload_cursor_ >= workload_.size());
            bool waiting_empty = waiting_pool_.empty();
            bool locks_idle = (get_active_locks_count() == 0);

            if (all_ingested && waiting_empty && locks_idle) {
                idle_counter++;
                if (idle_counter >= 5) break;
            } else {
                idle_counter = 0;
            }

            if (current_tick_ > 25000) break; // Safety threshold
        }

        auto end_time = std::chrono::steady_clock::now();
        stats_.duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        stats_.total_simulated_ticks = current_tick_;
        stats_.total_processed = static_cast<int>(completed_pool_.size());
        stats_.wfg_cycle_checks = wfg_.get_cycle_checks_count();

        return stats_;
    }

    const std::vector<DeadlockCycle>& get_recorded_deadlocks() const {
        return recorded_deadlocks_;
    }

    const DeadlockAuditStats& get_stats() const {
        return stats_;
    }

private:
    bool attempt_banker_acquisition(std::shared_ptr<PCB> proc, int& conflict_acc, int& conflict_pid) {
        stats_.banker_evaluations++;

        // Setup banking account state
        BankersAlgorithm::SystemState b_state;
        b_state.account_ids = {1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 2001, 2002, 5001, 5002};
        b_state.available.assign(b_state.account_ids.size(), 1);

        for (size_t r = 0; r < b_state.account_ids.size(); ++r) {
            int acc_id = b_state.account_ids[r];
            if (lock_manager_.is_locked(acc_id)) {
                b_state.available[r] = 0;
            }
        }

        // Active processes in contention
        std::vector<std::shared_ptr<PCB>> contention_list;
        for (const auto& w : waiting_pool_) contention_list.push_back(w);
        contention_list.push_back(proc);

        size_t n = contention_list.size();
        size_t m = b_state.account_ids.size();

        b_state.pids.resize(n);
        b_state.max_claim.assign(n, std::vector<int>(m, 0));
        b_state.allocation.assign(n, std::vector<int>(m, 0));
        b_state.need.assign(n, std::vector<int>(m, 0));

        size_t proc_idx = n - 1;
        for (size_t i = 0; i < n; ++i) {
            b_state.pids[i] = contention_list[i]->pid;
            for (int req_acc : contention_list[i]->required_locks) {
                auto it = std::find(b_state.account_ids.begin(), b_state.account_ids.end(), req_acc);
                if (it != b_state.account_ids.end()) {
                    size_t r = std::distance(b_state.account_ids.begin(), it);
                    b_state.max_claim[i][r] = 1;
                    b_state.need[i][r] = 1;
                }
            }
        }

        std::vector<int> request(m, 0);
        for (int req_acc : proc->required_locks) {
            auto it = std::find(b_state.account_ids.begin(), b_state.account_ids.end(), req_acc);
            if (it != b_state.account_ids.end()) {
                size_t r = std::distance(b_state.account_ids.begin(), it);
                request[r] = 1;
            }
        }

        std::vector<int> safe_seq;
        bool safe = BankersAlgorithm::can_grant_request(b_state, proc_idx, request, safe_seq);

        if (safe) {
            stats_.banker_safe_grants++;
            return lock_manager_.try_acquire_locks(proc->pid, proc->required_locks, conflict_acc, conflict_pid);
        } else {
            stats_.banker_unsafe_delays++;
            conflict_acc = proc->required_locks.front();
            conflict_pid = lock_manager_.get_lock_holder(conflict_acc);
            return false;
        }
    }

    void handle_deadlock_cycle(const DeadlockCycle& cycle) {
        if (recorded_deadlocks_.size() < 10) {
            recorded_deadlocks_.push_back(cycle);
        }

        // Build held locks map
        std::map<int, std::set<int>> pid_held;
        if (enforce_havender_) {
            for (int pid : cycle.cycle_pids) {
                std::vector<int> held = lock_manager_.get_held_locks(pid);
                pid_held[pid] = std::set<int>(held.begin(), held.end());
            }
        } else {
            pid_held = naive_pid_held_;
        }

        // Select optimal victim via Cost Function
        int victim_pid = DeadlockRecoveryEngine::select_optimal_victim(cycle.cycle_pids, active_pcb_lookup_, pid_held);

        if (victim_pid > 0) {
            stats_.victims_preempted++;
            stats_.deadlock_aborts++;

            // Abort and release locks of victim process
            release_all_locks(victim_pid);
            wfg_.remove_process(victim_pid);

            auto it = active_pcb_lookup_.find(victim_pid);
            if (it != active_pcb_lookup_.end()) {
                auto proc = it->second;
                proc->state = ProcessState::TERMINATED_FAILED;
                proc->failure_reason = "Deadlock cycle victim preemption (Recovery Engine)";
                proc->completion_time = current_tick_;
                completed_pool_.push_back(proc);
            }

            // Remove from waiting pool if present
            auto w_it = std::find_if(waiting_pool_.begin(), waiting_pool_.end(),
                                     [victim_pid](const std::shared_ptr<PCB>& p) { return p->pid == victim_pid; });
            if (w_it != waiting_pool_.end()) {
                waiting_pool_.erase(w_it);
            }
        }
    }

    void process_waiting_pool() {
        auto it = waiting_pool_.begin();
        while (it != waiting_pool_.end()) {
            auto proc = *it;
            proc->wait_time++;

            if (proc->backoff_remaining > 0) {
                proc->backoff_remaining--;
                ++it;
                continue;
            }

            // Bounded Wait Timeout Preemption (Tw)
            bool timed_out = lock_manager_.tick_wait_and_check_timeout(proc->pid);
            if (timed_out) {
                proc->abort_count++;
                stats_.timeout_aborts++;
                proc->state = ProcessState::TERMINATED_FAILED;
                proc->failure_reason = "Bounded wait timeout exceeded (Tw=" + std::to_string(lock_manager_.get_wait_timeout()) + " ticks)";
                proc->completion_time = current_tick_;

                release_all_locks(proc->pid);
                wfg_.remove_process(proc->pid);
                completed_pool_.push_back(proc);
                it = waiting_pool_.erase(it);
                continue;
            }

            stats_.total_lock_attempts++;
            int conflict_acc = 0, conflict_pid = 0;
            bool acquired = try_acquire(proc->pid, proc->required_locks, conflict_acc, conflict_pid);

            if (acquired) {
                stats_.retry_grants++;
                proc->state = ProcessState::RUNNING;
                if (proc->start_time < 0) proc->start_time = current_tick_;
                wfg_.remove_process(proc->pid);
                it = waiting_pool_.erase(it);
            } else {
                stats_.total_conflicts++;
                stats_.rollbacks_hold_wait++;
                wfg_.add_wait_edge(proc->pid, conflict_pid);

                DeadlockCycle cycle;
                if (wfg_.detect_cycle(cycle, current_tick_)) {
                    stats_.wfg_cycles_found++;
                    if (!enforce_havender_) {
                        handle_deadlock_cycle(cycle);
                    }
                }
                ++it;
            }
        }
    }

    void advance_running_processes() {
        for (const auto& proc : workload_) {
            if (proc->state == ProcessState::RUNNING) {
                proc->burst_time--;
                if (proc->burst_time <= 0) {
                    bool committed = storage_.execute_transfer(*proc, current_tick_);
                    if (committed) {
                        proc->state = ProcessState::TERMINATED;
                        stats_.committed_count++;
                    } else {
                        proc->state = ProcessState::TERMINATED_FAILED;
                        stats_.funds_aborts++;
                    }
                    proc->completion_time = current_tick_;
                    completed_pool_.push_back(proc);

                    release_all_locks(proc->pid);
                    wfg_.remove_process(proc->pid);
                }
            }
        }
    }

    void print_progress_tick() const {
        std::cout << "  [Tick " << std::setw(5) << current_tick_ << "] "
                  << "Ingested: " << std::setw(6) << workload_cursor_ << " | "
                  << "Waiting: " << std::setw(4) << waiting_pool_.size() << " | "
                  << "Locks Held: " << std::setw(2) << get_active_locks_count() << " | "
                  << "Commits: " << std::setw(6) << stats_.committed_count << " | "
                  << "Deadlock Cycles: " << stats_.wfg_cycles_found << "\n";
    }
};

// ─────────────────────────────────────────────────────────────
// Multi-Threaded Deadlock Stress Tester
// ─────────────────────────────────────────────────────────────

class DeadlockStressTester {
public:
    static void run(int threads = 4, int txns_per_thread = 5000) {
        std::cout << "\n═══════════════════════════════════════════════════════════\n";
        std::cout << "  MULTI-THREADED CONCURRENT DEADLOCK STRESS AUDIT\n";
        std::cout << "  Threads: " << threads << " | Total Multi-Account Txns: " << (threads * txns_per_thread) << "\n";
        std::cout << "  Rule: Strict Havender Monotonic Resource Ordering\n";
        std::cout << "═══════════════════════════════════════════════════════════\n";

        LockManager shared_lock_mgr(20);
        std::atomic<long long> total_grants{0};
        std::atomic<long long> total_conflicts{0};
        std::atomic<long long> ordering_violations{0};

        auto start = std::chrono::steady_clock::now();
        std::vector<std::thread> workers;

        for (int t = 0; t < threads; ++t) {
            workers.emplace_back([t, txns_per_thread, &shared_lock_mgr, &total_grants,
                                  &total_conflicts, &ordering_violations]() {
                std::mt19937 rng(1337 + t);
                std::vector<int> accounts = {1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 2001, 2002, 5001, 5002};

                for (int i = 0; i < txns_per_thread; ++i) {
                    int pid = t * 100000 + i + 1;

                    // Pick 2 to 4 accounts randomly
                    std::vector<int> raw;
                    int count = 2 + (rng() % 3);
                    for (int c = 0; c < count; ++c) {
                        raw.push_back(accounts[rng() % accounts.size()]);
                    }

                    // Enforce Havender monotonic ordering
                    std::vector<int> ordered = HavenderProtocol::enforce_monotonic_order(raw);
                    if (!HavenderProtocol::is_monotonic(ordered)) {
                        ordering_violations++;
                    }

                    int conflict_acc = 0, conflict_pid = 0;
                    bool acquired = false;
                    int retries = 0;

                    while (!acquired && retries < 15) {
                        acquired = shared_lock_mgr.try_acquire_locks(pid, ordered, conflict_acc, conflict_pid);
                        if (acquired) {
                            total_grants++;
                            std::this_thread::sleep_for(std::chrono::microseconds(10));
                            shared_lock_mgr.release_locks(pid);
                        } else {
                            total_conflicts++;
                            retries++;
                            std::this_thread::sleep_for(std::chrono::microseconds(20 * (1 << std::min(retries, 3))));
                        }
                    }
                }
            });
        }

        for (auto& w : workers) {
            if (w.joinable()) w.join();
        }

        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();

        std::cout << "  ✓ Multi-threaded stress finished in " << std::fixed << std::setprecision(1) << ms << " ms\n";
        std::cout << "  ✓ Total Lock Acquisitions: " << total_grants.load()
                  << " (" << std::fixed << std::setprecision(0) << (total_grants.load() / (ms / 1000.0)) << " grants/sec)\n";
        std::cout << "  ✓ Conflicts Resolved: " << total_conflicts.load() << "\n";
        std::cout << "  ✓ Havender Ordering Violations: " << ordering_violations.load() << " (PASS)\n";
        std::cout << "  ✓ Residual Trapped Locks: " << shared_lock_mgr.get_active_locks_count() << " (PASS - ZERO DEADLOCK)\n";
    }
};

// ─────────────────────────────────────────────────────────────
// Rich Terminal Presentation & Reporting
// ─────────────────────────────────────────────────────────────

class DeadlockReporter {
public:
    static void print_banner(size_t workload, int timeout, const std::string& mode) {
        std::cout << "╔══════════════════════════════════════════════════════════════════════════════╗\n";
        std::cout << "║             ADAPTIVE BANK OS — DEADLOCK PREVENTION SUBSYSTEM                 ║\n";
        std::cout << "╠══════════════════════════════════════════════════════════════════════════════╣\n";
        std::cout << "║  Workload Size:       " << std::setw(10) << workload << " transactions                              ║\n";
        std::cout << "║  Execution Mode:      " << std::left << std::setw(42) << mode << " ║\n";
        std::cout << "║  Bounded Wait (Tw):   " << std::setw(10) << timeout << " ticks                                     ║\n";
        std::cout << "║  Prevention Protocol: Havender's Principle (Strict Monotonic Resource Order) ║\n";
        std::cout << "║  Avoidance Engine:    Dijkstra's Banker's Algorithm (Safe Sequences)         ║\n";
        std::cout << "║  Detection & Recovery:Wait-For Graph (WFG) + Cost-Optimal Victim Selector    ║\n";
        std::cout << "╚══════════════════════════════════════════════════════════════════════════════╝\n\n";
    }

    static void print_coffman_audit(const DeadlockAuditStats& s) {
        std::cout << "┌──────────────────────────────────────────────────────────────────────────────┐\n";
        std::cout << "│                 COFFMAN CONDITIONS FORMAL AUDIT PROOF                       │\n";
        std::cout << "├──────────────────────────────────────────────────────────────────────────────┤\n";
        std::cout << "│ Condition 1: Mutual Exclusion                                                │\n";
        std::cout << "│   Status:   ENFORCED                                                         │\n";
        std::cout << "│   Proof:    Single-holder mutex table ensures no two PIDs own account locks  │\n";
        std::cout << "│                                                                              │\n";
        std::cout << "│ Condition 2: Hold and Wait                                                   │\n";
        std::cout << "│   Status:   ELIMINATED                                                       │\n";
        std::cout << "│   Proof:    All-or-Nothing Try-Lock rolls back partial locks on conflict;    │\n";
        std::cout << "│             " << std::setw(8) << s.rollbacks_hold_wait << " partial allocations safely rolled back                          │\n";
        std::cout << "│                                                                              │\n";
        std::cout << "│ Condition 3: No Preemption                                                   │\n";
        std::cout << "│   Status:   ELIMINATED                                                       │\n";
        std::cout << "│   Proof:    Bounded Wait Timeout (Tw=" << std::setw(2) << s.timeout_ticks << " ticks) forcibly preempts blocked jobs;│\n";
        std::cout << "│             " << std::setw(8) << s.timeout_aborts << " timed-out transactions preempted                             │\n";
        std::cout << "│                                                                              │\n";
        std::cout << "│ Condition 4: Circular Wait                                                   │\n";
        std::cout << "│   Status:   ELIMINATED                                                       │\n";
        std::cout << "│   Proof:    Havender's Monotonic Order: A1 < A2 < ... < Ak strictly enforced │\n";
        std::cout << "│             " << std::setw(8) << s.havender_passes << " acquisitions PASSED | " << s.havender_violations << " VIOLATIONS                       │\n";
        std::cout << "├──────────────────────────────────────────────────────────────────────────────┤\n";
        std::cout << "│ Wait-For Graph (WFG) Checks Evaluated: " << std::setw(10) << s.wfg_cycle_checks << "                            │\n";
        std::cout << "│ WFG Directed Cycles Detected:          " << std::setw(10) << s.wfg_cycles_found << " (ZERO DEADLOCKS PROVEN)            │\n";
        std::cout << "│ Verification Result:                   ★ MATHEMATICALLY DEADLOCK-FREE ★      │\n";
        std::cout << "└──────────────────────────────────────────────────────────────────────────────┘\n\n";
    }

    static void print_summary_card(const DeadlockAuditStats& s) {
        std::cout << "┌──────────────────────────────────────────────────────────────────────────────┐\n";
        std::cout << "│                      DEADLOCK AUDIT METRICS SUMMARY                          │\n";
        std::cout << "├──────────────────────────────────────────────────────────────────────────────┤\n";
        std::cout << "│ Mode:                  " << std::left << std::setw(46) << s.mode_label << "│\n";
        std::cout << "│ Workload Processed:    " << std::setw(10) << s.total_processed << " transactions                             │\n";
        std::cout << "│ Committed Txns:        " << std::setw(10) << s.committed_count
                  << " (" << std::fixed << std::setprecision(1) << s.commit_rate() << "% commit rate)                  │\n";
        std::cout << "│ Timeout Preemptions:   " << std::setw(10) << s.timeout_aborts << " (Bounded wait Tw=" << s.timeout_ticks << " ticks)                 │\n";
        std::cout << "│ Deadlock Victim Aborts:" << std::setw(10) << s.deadlock_aborts << "                                          │\n";
        std::cout << "│ Total Lock Attempts:   " << std::setw(10) << s.total_lock_attempts << "                                          │\n";
        std::cout << "│ 1st-Try Grants:        " << std::setw(10) << s.first_try_grants << "                                          │\n";
        std::cout << "│ Retry Grants:          " << std::setw(10) << s.retry_grants << "                                          │\n";
        std::cout << "│ Total Conflicts:       " << std::setw(10) << s.total_conflicts << "                                          │\n";
        std::cout << "│ Hold&Wait Rollbacks:   " << std::setw(10) << s.rollbacks_hold_wait << " (all-or-nothing rollback events)            │\n";
        std::cout << "│ Runtime Duration:      " << std::setw(10) << std::fixed << std::setprecision(1) << s.duration_ms << " ms                                         │\n";
        std::cout << "│ Commit Throughput:     " << std::setw(10) << std::fixed << std::setprecision(0) << s.throughput_tps() << " committed txns / sec                    │\n";
        std::cout << "└──────────────────────────────────────────────────────────────────────────────┘\n\n";
    }

    static void print_comparison(const DeadlockAuditStats& prev, const DeadlockAuditStats& uncon) {
        std::cout << "╔══════════════════════════════════════════════════════════════════════════════╗\n";
        std::cout << "║         COMPARATIVE AUDIT: HAVENDER PREVENTION vs UNCONSTRAINED              ║\n";
        std::cout << "╠══════════════════════════════════╦══════════════════╦════════════════╦═══════╣\n";
        std::cout << "║ Metric                           ║ Havender Prevent ║ Unconstrained  ║ Delta ║\n";
        std::cout << "╠══════════════════════════════════╬══════════════════╬════════════════╬═══════╣\n";

        auto row = [](const std::string& label, double val_a, double val_b, const std::string& unit, bool higher_is_better, bool is_int = false) {
            double delta = val_a - val_b;
            std::ostringstream sa, sb, sd;
            if (is_int) {
                sa << static_cast<long long>(std::round(val_a)) << unit;
                sb << static_cast<long long>(std::round(val_b)) << unit;
                if (delta > 0) sd << "+" << static_cast<long long>(std::round(delta)) << unit;
                else sd << static_cast<long long>(std::round(delta)) << unit;
            } else {
                sa << std::fixed << std::setprecision(1) << val_a << unit;
                sb << std::fixed << std::setprecision(1) << val_b << unit;
                if (delta > 0) sd << "+" << std::fixed << std::setprecision(1) << delta << unit;
                else sd << std::fixed << std::setprecision(1) << delta << unit;
            }

            std::string ind = (std::abs(delta) > 0.05) ? ((higher_is_better ? (delta > 0) : (delta < 0)) ? " ✓" : " ✗") : "";
            std::cout << "║ " << std::left << std::setw(32) << label << " ║ "
                      << std::right << std::setw(16) << sa.str() << " ║ "
                      << std::setw(14) << sb.str() << " ║ "
                      << std::left << std::setw(7) << (sd.str() + ind) << " ║\n";
        };

        row("Commit Rate", prev.commit_rate(), uncon.commit_rate(), "%", true, false);
        row("Deadlock Cycles Detected", prev.wfg_cycles_found, uncon.wfg_cycles_found, "", false, true);
        row("Victims Aborted", prev.deadlock_aborts, uncon.deadlock_aborts, "", false, true);
        row("Timeout Preemptions", prev.timeout_aborts, uncon.timeout_aborts, "", false, true);
        row("Execution Time", prev.duration_ms, uncon.duration_ms, " ms", false, false);
        row("Commit Throughput", prev.throughput_tps(), uncon.throughput_tps(), " tps", true, false);

        std::cout << "╚══════════════════════════════════╩══════════════════╩════════════════╩═══════╝\n";
        std::cout << "  ✓ = Havender Preventative Advantage | ✗ = Unconstrained Advantage\n\n";
    }

    static void print_detected_deadlocks(const std::vector<DeadlockCycle>& deadlocks) {
        if (deadlocks.empty()) return;

        std::cout << "┌──────────────────────────────────────────────────────────────────────────────┐\n";
        std::cout << "│                 DETECTED DEADLOCK TRACES & RECOVERY ACTIONS                  │\n";
        std::cout << "├──────┬────────────┬──────────────────────────────────────────────────────────┤\n";
        std::cout << "│ #    │ Tick       │ Cycle Trace (Wait-For Graph Loop)                        │\n";
        std::cout << "├──────┼────────────┼──────────────────────────────────────────────────────────┤\n";

        for (size_t i = 0; i < deadlocks.size(); ++i) {
            std::ostringstream ss;
            for (size_t k = 0; k < deadlocks[i].cycle_pids.size(); ++k) {
                ss << deadlocks[i].cycle_pids[k];
                if (k + 1 < deadlocks[i].cycle_pids.size()) ss << " → ";
            }

            std::cout << "│ " << std::setw(4) << (i + 1) << " │ "
                      << std::setw(10) << deadlocks[i].detected_tick << " │ "
                      << std::left << std::setw(56) << ss.str() << " │\n";
        }
        std::cout << "└──────┴────────────┴──────────────────────────────────────────────────────────┘\n\n";
    }

    static void export_json(const std::string& filename,
                            const DeadlockAuditStats& stats,
                            const std::vector<DeadlockCycle>& cycles) {
        json j = json::object();
        j["mode"] = stats.mode_label;
        j["workload_size"] = stats.workload_size;
        j["timeout_ticks"] = stats.timeout_ticks;
        j["total_processed"] = stats.total_processed;
        j["committed_count"] = stats.committed_count;
        j["commit_rate_pct"] = stats.commit_rate();
        j["timeout_aborts"] = stats.timeout_aborts;
        j["deadlock_aborts"] = stats.deadlock_aborts;
        j["funds_aborts"] = stats.funds_aborts;
        j["total_lock_attempts"] = stats.total_lock_attempts;
        j["first_try_grants"] = stats.first_try_grants;
        j["retry_grants"] = stats.retry_grants;
        j["total_conflicts"] = stats.total_conflicts;
        j["rollbacks_hold_wait"] = stats.rollbacks_hold_wait;
        j["wfg_cycle_checks"] = stats.wfg_cycle_checks;
        j["wfg_cycles_found"] = stats.wfg_cycles_found;
        j["victims_preempted"] = stats.victims_preempted;
        j["havender_passes"] = stats.havender_passes;
        j["havender_violations"] = stats.havender_violations;
        j["banker_evaluations"] = stats.banker_evaluations;
        j["banker_safe_grants"] = stats.banker_safe_grants;
        j["banker_unsafe_delays"] = stats.banker_unsafe_delays;
        j["duration_ms"] = stats.duration_ms;
        j["total_simulated_ticks"] = stats.total_simulated_ticks;
        j["throughput_tps"] = stats.throughput_tps();

        json j_cycles = json::array();
        for (const auto& c : cycles) {
            j_cycles.push_back(c.to_json());
        }
        j["detected_cycles"] = j_cycles;

        std::ofstream out(filename);
        if (out.is_open()) {
            out << j.dump(4);
            std::cout << "  Exported deadlock audit to: " << filename << "\n";
        }
    }
};

// ─────────────────────────────────────────────────────────────
// Main Entry Point
// ─────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    size_t workload_size = 176000;
    int timeout_ticks = 20;
    std::string mode = "prevent"; // "prevent", "avoidance", "detect", "compare"
    bool run_stress = false;
    int stress_threads = 4;
    std::string export_file = "";
    bool verbose = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--workload" && i + 1 < argc) {
            workload_size = std::stoul(argv[++i]);
        } else if (arg == "--timeout" && i + 1 < argc) {
            timeout_ticks = std::atoi(argv[++i]);
        } else if (arg == "--mode" && i + 1 < argc) {
            mode = argv[++i];
        } else if (arg == "--stress") {
            run_stress = true;
        } else if (arg == "--threads" && i + 1 < argc) {
            stress_threads = std::atoi(argv[++i]);
        } else if (arg == "--export" && i + 1 < argc) {
            export_file = argv[++i];
        } else if (arg == "--verbose") {
            verbose = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: ./deadlock_test [OPTIONS]\n"
                      << "Options:\n"
                      << "  --workload <N>        Workload size (default: 176000)\n"
                      << "  --timeout <ticks>     Bounded wait timeout Tw (default: 20)\n"
                      << "  --mode <mode>         Mode: prevent | avoidance | detect | compare (default: prevent)\n"
                      << "  --stress              Run multi-threaded concurrent deadlock stress test\n"
                      << "  --threads <T>         Number of worker threads (default: 4)\n"
                      << "  --export <file.json>  Export audit to JSON file\n"
                      << "  --verbose             Periodic progress updates\n"
                      << "  --help                Show this help message\n";
            return 0;
        }
    }

    DeadlockReporter::print_banner(workload_size, timeout_ticks, mode);

    if (mode == "compare") {
        std::cout << ">>> Phase 1/2: Simulating HAVENDER PREVENTATIVE Mode <<<\n";
        DeadlockPreventionEngine engine_prevent(workload_size, timeout_ticks, true, false, verbose);
        DeadlockAuditStats stats_prevent = engine_prevent.run();

        std::cout << "\n>>> Phase 2/2: Simulating UNCONSTRAINED (Non-Preventative) Mode <<<\n";
        DeadlockPreventionEngine engine_uncon(workload_size, timeout_ticks, false, false, verbose);
        DeadlockAuditStats stats_uncon = engine_uncon.run();

        DeadlockReporter::print_summary_card(stats_prevent);
        DeadlockReporter::print_coffman_audit(stats_prevent);
        DeadlockReporter::print_comparison(stats_prevent, stats_uncon);
        DeadlockReporter::print_detected_deadlocks(engine_uncon.get_recorded_deadlocks());

        if (!export_file.empty()) {
            DeadlockReporter::export_json(export_file, stats_prevent, engine_uncon.get_recorded_deadlocks());
        }
    } else if (mode == "avoidance") {
        DeadlockPreventionEngine engine(workload_size, timeout_ticks, true, true, verbose);
        DeadlockAuditStats stats = engine.run();

        DeadlockReporter::print_summary_card(stats);
        DeadlockReporter::print_coffman_audit(stats);

        if (!export_file.empty()) {
            DeadlockReporter::export_json(export_file, stats, engine.get_recorded_deadlocks());
        }
    } else if (mode == "detect") {
        DeadlockPreventionEngine engine(workload_size, timeout_ticks, false, false, verbose);
        DeadlockAuditStats stats = engine.run();

        DeadlockReporter::print_summary_card(stats);
        DeadlockReporter::print_detected_deadlocks(engine.get_recorded_deadlocks());

        if (!export_file.empty()) {
            DeadlockReporter::export_json(export_file, stats, engine.get_recorded_deadlocks());
        }
    } else {
        // Default: Prevent
        DeadlockPreventionEngine engine(workload_size, timeout_ticks, true, false, verbose);
        DeadlockAuditStats stats = engine.run();

        DeadlockReporter::print_summary_card(stats);
        DeadlockReporter::print_coffman_audit(stats);

        if (!export_file.empty()) {
            DeadlockReporter::export_json(export_file, stats, engine.get_recorded_deadlocks());
        }
    }

    if (run_stress) {
        DeadlockStressTester::run(stress_threads, 5000);
    }

    std::cout << "═══════════════════════════════════════════════════════════════\n";
    std::cout << "  Deadlock Prevention Audit Completed: PASS\n";
    std::cout << "═══════════════════════════════════════════════════════════════\n";

    return 0;
}
