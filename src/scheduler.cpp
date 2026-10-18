/**
 * scheduler.cpp — Standalone MLFQ Scheduler for the Adaptive Bank OS
 *
 * Generates the 176K transaction workload (PCBs from workload_generator.hpp),
 * runs every transaction through the complete scheduling pipeline:
 *   MLFQ dispatch → Lock acquisition → CPU execution → ACID commit/rollback
 * and produces detailed per-type performance analytics, state-transition
 * statistics, and an optional JSON export of all transitions.
 *
 * Build:
 *   g++ -std=c++17 -O2 -pthread -I ../include scheduler.cpp -o scheduler
 *
 * Usage:
 *   ./scheduler                          # defaults: 176000 txns, aging=50, quantum=5
 *   ./scheduler --workload 50000         # custom workload size
 *   ./scheduler --aging 30 --quantum 8   # custom MLFQ parameters
 *   ./scheduler --export results.json    # export transitions to JSON file
 *   ./scheduler --verbose                # print per-tick progress every 500 ticks
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

#include "pcb.hpp"
#include "workload_generator.hpp"
#include "mlfq_scheduler.hpp"
#include "lock_manager.hpp"
#include "middleware.hpp"
#include "mysql_storage.hpp"

// ─────────────────────────────────────────────────────────────
// Scheduling Statistics Collector
// ─────────────────────────────────────────────────────────────

struct SchedulerStats {
    // Per-type counters
    int upi_total = 0,   atm_total = 0,   salary_total = 0;
    int upi_commit = 0,  atm_commit = 0,  salary_commit = 0;
    int upi_fail = 0,    atm_fail = 0,    salary_fail = 0;

    // Aggregate timing (completed processes only)
    long long total_turnaround = 0;  // completion_time - arrival_time
    long long total_wait = 0;        // wait_time accumulated while in queues
    long long total_response = 0;    // start_time - arrival_time

    int completed_count = 0;
    int failed_count = 0;

    // Queue utilization
    long long q1_dispatches = 0;
    long long q2_dispatches = 0;
    long long preemptions = 0;
    long long aged_promotions = 0;

    // Lock contention
    long long lock_conflicts = 0;
    long long lock_timeouts = 0;
    long long total_retries = 0;

    // Per-tick peak tracking
    int peak_q1_size = 0;
    int peak_q2_size = 0;
    int peak_waiting_size = 0;

    // Throughput tracking
    int first_commit_tick = -1;
    int last_commit_tick = -1;

    void record_completed(const PCB& pcb) {
        int turnaround = pcb.completion_time - pcb.arrival_time;
        int response = (pcb.start_time >= 0) ? (pcb.start_time - pcb.arrival_time) : 0;

        total_turnaround += turnaround;
        total_wait += pcb.wait_time;
        total_response += response;
        total_retries += pcb.retry_count;

        switch (pcb.type) {
            case TransactionType::UPI:
                if (pcb.state == ProcessState::TERMINATED) upi_commit++;
                else upi_fail++;
                break;
            case TransactionType::ATM:
                if (pcb.state == ProcessState::TERMINATED) atm_commit++;
                else atm_fail++;
                break;
            case TransactionType::SALARY:
                if (pcb.state == ProcessState::TERMINATED) salary_commit++;
                else salary_fail++;
                break;
        }

        if (pcb.state == ProcessState::TERMINATED) {
            completed_count++;
            if (first_commit_tick < 0) first_commit_tick = pcb.completion_time;
            last_commit_tick = pcb.completion_time;
        } else {
            failed_count++;
        }
    }

    void update_peaks(size_t q1, size_t q2, size_t waiting) {
        if (static_cast<int>(q1) > peak_q1_size) peak_q1_size = static_cast<int>(q1);
        if (static_cast<int>(q2) > peak_q2_size) peak_q2_size = static_cast<int>(q2);
        if (static_cast<int>(waiting) > peak_waiting_size) peak_waiting_size = static_cast<int>(waiting);
    }

    double avg_turnaround() const { return (completed_count + failed_count > 0) ? static_cast<double>(total_turnaround) / (completed_count + failed_count) : 0.0; }
    double avg_wait()       const { return (completed_count + failed_count > 0) ? static_cast<double>(total_wait)       / (completed_count + failed_count) : 0.0; }
    double avg_response()   const { return (completed_count + failed_count > 0) ? static_cast<double>(total_response)   / (completed_count + failed_count) : 0.0; }

    double commit_rate() const {
        int total = completed_count + failed_count;
        return (total > 0) ? (static_cast<double>(completed_count) / total) * 100.0 : 0.0;
    }

    double effective_tps() const {
        if (first_commit_tick < 0 || last_commit_tick <= first_commit_tick) return 0.0;
        return static_cast<double>(completed_count) / (last_commit_tick - first_commit_tick);
    }
};

// ─────────────────────────────────────────────────────────────
// Standalone Scheduler Engine
// ─────────────────────────────────────────────────────────────

class SchedulerEngine {
private:
    // Workload dataset (from WorkloadGenerator)
    std::vector<std::shared_ptr<PCB>> workload_;
    size_t workload_cursor_ = 0;

    // Core subsystems
    MLFQScheduler   scheduler_;
    LockManager     lock_manager_;
    MDACCMiddleware  middleware_;
    StorageEngine   storage_;

    // Process pools
    std::vector<std::shared_ptr<PCB>> waiting_pool_;
    std::vector<std::shared_ptr<PCB>> completed_pool_;

    // Simulation state
    int current_tick_ = 0;
    int max_idle_ticks_ = 500; // Stop after N ticks with no activity

    // Statistics
    SchedulerStats stats_;

    // Configuration
    bool verbose_ = false;
    int verbose_interval_ = 500;

public:
    SchedulerEngine(size_t workload_size, int aging_threshold, int q2_quantum,
                    bool adaptive_mode, bool verbose)
        : scheduler_(aging_threshold, q2_quantum),
          lock_manager_(20),
          middleware_(adaptive_mode, 100),
          verbose_(verbose) {

        // Generate the full transaction workload dataset
        std::cout << "Generating " << workload_size << " transaction PCBs...\n";
        auto t_start = std::chrono::steady_clock::now();

        workload_ = WorkloadGenerator::generate_workload(workload_size, 0, 8000, 42);

        auto t_end = std::chrono::steady_clock::now();
        double gen_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
        std::cout << "  Workload generated in " << std::fixed << std::setprecision(1) << gen_ms << " ms\n";

        // Count types in the dataset
        for (const auto& pcb : workload_) {
            switch (pcb->type) {
                case TransactionType::UPI:    stats_.upi_total++; break;
                case TransactionType::ATM:    stats_.atm_total++; break;
                case TransactionType::SALARY: stats_.salary_total++; break;
            }
        }

        std::cout << "  Dataset: " << stats_.upi_total << " UPI ("
                  << std::fixed << std::setprecision(1) << (100.0 * stats_.upi_total / workload_.size()) << "%), "
                  << stats_.atm_total << " ATM ("
                  << (100.0 * stats_.atm_total / workload_.size()) << "%), "
                  << stats_.salary_total << " Salary ("
                  << (100.0 * stats_.salary_total / workload_.size()) << "%)\n\n";
    }

    // ── Main Scheduling Loop ───────────────────────────────────
    void run() {
        std::cout << "═══════════════════════════════════════════════════════════\n";
        std::cout << "  ADAPTIVE BANK OS — STANDALONE SCHEDULER ENGINE\n";
        std::cout << "  Mode: " << (middleware_.is_adaptive_mode() ? "ADAPTIVE (MD-ACC)" : "STANDARD BASELINE") << "\n";
        std::cout << "  Aging Threshold: " << scheduler_.get_aging_threshold() << " ticks\n";
        std::cout << "  Q2 Quantum: " << scheduler_.get_q2_quantum() << " ticks\n";
        std::cout << "  Lock Wait Timeout (Tw): " << lock_manager_.get_wait_timeout() << " ticks\n";
        std::cout << "═══════════════════════════════════════════════════════════\n\n";

        auto sim_start = std::chrono::steady_clock::now();
        int idle_counter = 0;

        while (true) {
            current_tick_++;

            // ── Step 1: Advance scheduler wait timers ──
            scheduler_.on_tick_advance();

            // ── Step 2: Anti-Starvation Aging — promote Q2 → Q1 ──
            size_t q1_before = scheduler_.get_q1_size();
            scheduler_.promote_aged_processes(current_tick_);
            size_t q1_after = scheduler_.get_q1_size();
            if (q1_after > q1_before) {
                stats_.aged_promotions += static_cast<long long>(q1_after - q1_before);
            }

            // ── Step 3: Ingest arriving transactions from workload ──
            int arrivals_this_tick = 0;
            while (workload_cursor_ < workload_.size() &&
                   workload_[workload_cursor_]->arrival_time <= current_tick_) {
                auto proc = workload_[workload_cursor_++];
                middleware_.record_arrival(proc->type);
                scheduler_.add_process(proc, current_tick_);
                arrivals_this_tick++;
            }

            // ── Step 4: Process waiting pool (backoff & lock retry) ──
            process_waiting_pool();

            // ── Step 5: Check preemption ──
            scheduler_.should_preempt();

            // ── Step 6: Dispatch next process from MLFQ ──
            std::shared_ptr<PCB> running = scheduler_.schedule_next(current_tick_);

            // ── Step 7: Execute CPU cycle + lock acquisition ──
            if (running) {
                execute_process(running);
            }

            // ── Step 8: Update telemetry ──
            int active_ql = static_cast<int>(scheduler_.get_q1_size() + scheduler_.get_q2_size() + waiting_pool_.size());
            middleware_.update_queue_length(active_ql);
            middleware_.advance_tick();

            // ── Step 9: Track peak queue sizes ──
            stats_.update_peaks(scheduler_.get_q1_size(), scheduler_.get_q2_size(), waiting_pool_.size());

            // ── Step 10: Verbose progress ──
            if (verbose_ && current_tick_ % verbose_interval_ == 0) {
                print_tick_progress();
            }

            // ── Termination check ──
            bool all_ingested = (workload_cursor_ >= workload_.size());
            bool queues_empty = (scheduler_.get_q1_size() == 0 && scheduler_.get_q2_size() == 0);
            bool no_running = (scheduler_.get_running_process() == nullptr);
            bool no_waiting = waiting_pool_.empty();

            if (all_ingested && queues_empty && no_running && no_waiting) {
                idle_counter++;
                if (idle_counter >= 10) {
                    break; // All transactions processed
                }
            } else {
                idle_counter = 0;
            }

            // Safety: prevent infinite loop if stuck
            if (current_tick_ > 50000) {
                std::cout << "\n[WARNING] Simulation exceeded 50,000 ticks. Stopping.\n";
                break;
            }
        }

        auto sim_end = std::chrono::steady_clock::now();
        double sim_ms = std::chrono::duration<double, std::milli>(sim_end - sim_start).count();

        std::cout << "\nSimulation completed at tick " << current_tick_
                  << " in " << std::fixed << std::setprecision(1) << sim_ms << " ms\n";

        // Collect stats from all completed processes
        for (const auto& pcb : completed_pool_) {
            stats_.record_completed(*pcb);
        }
    }

    // ── Waiting Pool Processing ────────────────────────────────
    void process_waiting_pool() {
        auto it = waiting_pool_.begin();
        while (it != waiting_pool_.end()) {
            auto proc = *it;

            // Serve backoff delay
            if (proc->backoff_remaining > 0) {
                proc->backoff_remaining--;
                ++it;
                continue;
            }

            // Check lock wait timeout (Tw)
            bool timed_out = lock_manager_.tick_wait_and_check_timeout(proc->pid);
            if (timed_out) {
                proc->abort_count++;
                middleware_.record_abort();
                stats_.lock_timeouts++;

                if (proc->retry_count >= 5 || !middleware_.is_adaptive_mode()) {
                    // Permanent failure
                    proc->state = ProcessState::TERMINATED_FAILED;
                    proc->failure_reason = "Lock wait timeout (Tw=" +
                        std::to_string(lock_manager_.get_wait_timeout()) + " ticks) exceeded";
                    proc->completion_time = current_tick_;
                    lock_manager_.release_locks(proc->pid);
                    completed_pool_.push_back(proc);
                    scheduler_.log_transition(current_tick_, proc->pid, proc->type,
                        ProcessState::WAITING, ProcessState::TERMINATED_FAILED, proc->failure_reason);
                    it = waiting_pool_.erase(it);
                    continue;
                } else {
                    // Retry with backoff
                    proc->retry_count++;
                    proc->backoff_remaining = middleware_.calculate_backoff(proc->pid, proc->retry_count);
                    lock_manager_.reset_wait_timer(proc->pid);
                    scheduler_.log_transition(current_tick_, proc->pid, proc->type,
                        ProcessState::WAITING, ProcessState::WAITING,
                        "Retry " + std::to_string(proc->retry_count) +
                        " with backoff " + std::to_string(proc->backoff_remaining) + " ticks");
                    ++it;
                    continue;
                }
            }

            // Try acquiring locks (strict monotonic order)
            middleware_.record_attempt();
            int conflict_acc = 0, conflict_pid = 0;
            bool acquired = lock_manager_.try_acquire_locks(proc->pid, proc->required_locks, conflict_acc, conflict_pid);

            if (acquired) {
                proc->state = ProcessState::READY;
                scheduler_.add_process(proc, current_tick_);
                scheduler_.log_transition(current_tick_, proc->pid, proc->type,
                    ProcessState::WAITING, ProcessState::READY, "Acquired locks; returned to scheduler");
                it = waiting_pool_.erase(it);
            } else {
                middleware_.record_conflict();
                stats_.lock_conflicts++;
                ++it;
            }
        }
    }

    // ── CPU Execution ──────────────────────────────────────────
    void execute_process(std::shared_ptr<PCB> proc) {
        // Attempt to acquire all required locks
        int conflict_acc = 0, conflict_pid = 0;
        middleware_.record_attempt();
        bool acquired = lock_manager_.try_acquire_locks(proc->pid, proc->required_locks, conflict_acc, conflict_pid);

        if (!acquired) {
            // Lock conflict → move to waiting pool with backoff
            middleware_.record_conflict();
            stats_.lock_conflicts++;

            proc->state = ProcessState::WAITING;
            proc->retry_count++;
            proc->backoff_remaining = middleware_.calculate_backoff(proc->pid, proc->retry_count);
            waiting_pool_.push_back(proc);
            scheduler_.clear_running_process();

            std::string reason = "Lock conflict on Account " + std::to_string(conflict_acc) +
                                 " held by PID " + std::to_string(conflict_pid);
            if (proc->backoff_remaining > 0) {
                reason += "; backoff " + std::to_string(proc->backoff_remaining) + " ticks";
            }
            scheduler_.log_transition(current_tick_, proc->pid, proc->type,
                ProcessState::RUNNING, ProcessState::WAITING, reason);

            // Track which queue dispatched this
            if (proc->current_queue == 1) stats_.q1_dispatches++;
            else stats_.q2_dispatches++;
            stats_.preemptions++;
            return;
        }

        // Track dispatch queue
        if (proc->current_queue == 1) stats_.q1_dispatches++;
        else stats_.q2_dispatches++;

        // Execute 1 CPU cycle
        proc->burst_time--;

        // Check if execution is complete
        if (proc->burst_time <= 0) {
            // Execute ACID transfer via storage engine
            bool success = storage_.execute_transfer(*proc, current_tick_);

            if (success) {
                middleware_.record_commit();
                scheduler_.log_transition(current_tick_, proc->pid, proc->type,
                    ProcessState::RUNNING, ProcessState::TERMINATED,
                    "Committed transfer successfully");
            } else {
                middleware_.record_abort();
                scheduler_.log_transition(current_tick_, proc->pid, proc->type,
                    ProcessState::RUNNING, ProcessState::TERMINATED_FAILED,
                    proc->failure_reason);
            }

            // Release all locks and move to completed pool
            lock_manager_.release_locks(proc->pid);
            completed_pool_.push_back(proc);
            scheduler_.clear_running_process();
        }
    }

    // ── Progress Logging ───────────────────────────────────────
    void print_tick_progress() const {
        size_t processed = completed_pool_.size();
        double pct = (workload_.size() > 0)
            ? (100.0 * processed / workload_.size()) : 0.0;

        std::cout << "  [Tick " << std::setw(6) << current_tick_ << "] "
                  << "Processed: " << std::setw(7) << processed << "/" << workload_.size()
                  << " (" << std::fixed << std::setprecision(1) << pct << "%) | "
                  << "Q1: " << std::setw(5) << scheduler_.get_q1_size()
                  << " Q2: " << std::setw(5) << scheduler_.get_q2_size()
                  << " Wait: " << std::setw(5) << waiting_pool_.size()
                  << " | Commits: " << storage_.get_total_commits()
                  << " Rollbacks: " << storage_.get_total_rollbacks()
                  << "\n";
    }

    // ── Results Reporting ──────────────────────────────────────
    void print_report() const {
        std::cout << "\n";
        std::cout << "╔═══════════════════════════════════════════════════════════════╗\n";
        std::cout << "║              SCHEDULING PERFORMANCE REPORT                    ║\n";
        std::cout << "╚═══════════════════════════════════════════════════════════════╝\n\n";

        // ── Overall Summary ──
        std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│  OVERALL SUMMARY                                            │\n";
        std::cout << "├─────────────────────────────────────────────────────────────┤\n";
        std::cout << "│  Total Transactions:     " << std::setw(10) << workload_.size() << "                            │\n";
        std::cout << "│  Committed (Success):    " << std::setw(10) << stats_.completed_count
                  << "  (" << std::fixed << std::setprecision(1) << stats_.commit_rate() << "%)                   │\n";
        std::cout << "│  Failed (Rollback):      " << std::setw(10) << stats_.failed_count
                  << "  (" << std::setprecision(1) << (100.0 - stats_.commit_rate()) << "%)                   │\n";
        std::cout << "│  Simulation Ticks:       " << std::setw(10) << current_tick_ << "                            │\n";
        std::cout << "│  Effective TPS:          " << std::setw(10) << std::setprecision(2) << stats_.effective_tps()
                  << "  txns/tick               │\n";
        std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

        // ── Per-Type Breakdown ──
        std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│  PER-TYPE BREAKDOWN                                         │\n";
        std::cout << "├─────────┬──────────┬──────────┬──────────┬─────────────────┤\n";
        std::cout << "│  Type   │  Total   │  Commit  │  Failed  │  Success Rate   │\n";
        std::cout << "├─────────┼──────────┼──────────┼──────────┼─────────────────┤\n";

        auto print_type_row = [](const std::string& name, int total, int commit, int fail) {
            double rate = (total > 0) ? (100.0 * commit / total) : 0.0;
            std::cout << "│  " << std::setw(5) << name << "  │"
                      << std::setw(8) << total << "  │"
                      << std::setw(8) << commit << "  │"
                      << std::setw(8) << fail << "  │"
                      << std::setw(11) << std::fixed << std::setprecision(1) << rate << "%     │\n";
        };

        print_type_row("UPI",    stats_.upi_total,    stats_.upi_commit,    stats_.upi_fail);
        print_type_row("ATM",    stats_.atm_total,    stats_.atm_commit,    stats_.atm_fail);
        print_type_row("SALARY", stats_.salary_total, stats_.salary_commit, stats_.salary_fail);

        std::cout << "└─────────┴──────────┴──────────┴──────────┴─────────────────┘\n\n";

        // ── Timing Metrics ──
        std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│  TIMING METRICS (averaged over all completed transactions)  │\n";
        std::cout << "├─────────────────────────────────────────────────────────────┤\n";
        std::cout << "│  Avg Turnaround Time:    " << std::setw(10) << std::setprecision(2) << stats_.avg_turnaround()
                  << " ticks                      │\n";
        std::cout << "│  Avg Wait Time:          " << std::setw(10) << std::setprecision(2) << stats_.avg_wait()
                  << " ticks                      │\n";
        std::cout << "│  Avg Response Time:      " << std::setw(10) << std::setprecision(2) << stats_.avg_response()
                  << " ticks                      │\n";
        std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

        // ── Scheduler Queue Stats ──
        std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│  MLFQ SCHEDULER STATISTICS                                  │\n";
        std::cout << "├─────────────────────────────────────────────────────────────┤\n";
        std::cout << "│  Q1 (SRTF) Dispatches:   " << std::setw(10) << stats_.q1_dispatches << "                            │\n";
        std::cout << "│  Q2 (RR) Dispatches:     " << std::setw(10) << stats_.q2_dispatches << "                            │\n";
        std::cout << "│  Preemptions:            " << std::setw(10) << stats_.preemptions << "                            │\n";
        std::cout << "│  Aged Promotions (Q2→Q1):" << std::setw(10) << stats_.aged_promotions << "                            │\n";
        std::cout << "│  Peak Q1 Size:           " << std::setw(10) << stats_.peak_q1_size << "                            │\n";
        std::cout << "│  Peak Q2 Size:           " << std::setw(10) << stats_.peak_q2_size << "                            │\n";
        std::cout << "│  Peak Waiting Pool:      " << std::setw(10) << stats_.peak_waiting_size << "                            │\n";
        std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

        // ── Lock Contention Stats ──
        std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│  LOCK CONTENTION & CONCURRENCY                              │\n";
        std::cout << "├─────────────────────────────────────────────────────────────┤\n";
        std::cout << "│  Total Lock Conflicts:   " << std::setw(10) << stats_.lock_conflicts << "                            │\n";
        std::cout << "│  Lock Wait Timeouts:     " << std::setw(10) << stats_.lock_timeouts << "                            │\n";
        std::cout << "│  Total Retries:          " << std::setw(10) << stats_.total_retries << "                            │\n";
        std::cout << "│  Total Commits:          " << std::setw(10) << storage_.get_total_commits() << "                            │\n";
        std::cout << "│  Total Rollbacks:        " << std::setw(10) << storage_.get_total_rollbacks() << "                            │\n";
        std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

        // ── Final Account Balances ──
        std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│  FINAL ACCOUNT BALANCES                                     │\n";
        std::cout << "├─────────────────────────────────────────────────────────────┤\n";

        auto balances = storage_.get_all_balances();
        for (const auto& [acc_id, balance] : balances) {
            std::cout << "│  Account " << std::setw(4) << acc_id << ":  $"
                      << std::setw(15) << std::fixed << std::setprecision(2) << balance
                      << "                           │\n";
        }
        std::cout << "└─────────────────────────────────────────────────────────────┘\n";
    }

    // ── JSON Export ────────────────────────────────────────────
    bool export_transitions(const std::string& filename, size_t max_records = 0) const {
        return scheduler_.export_transitions_to_json(filename, max_records);
    }

    const SchedulerStats& get_stats() const { return stats_; }
};

// ─────────────────────────────────────────────────────────────
// Main Entry Point
// ─────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    // Default parameters
    size_t workload_size = 176000;
    int aging_threshold = 50;
    int q2_quantum = 5;
    bool adaptive_mode = true;
    bool verbose = false;
    std::string export_file;

    // Parse command-line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--workload" && i + 1 < argc) {
            workload_size = std::stoul(argv[++i]);
        } else if (arg == "--aging" && i + 1 < argc) {
            aging_threshold = std::stoi(argv[++i]);
        } else if (arg == "--quantum" && i + 1 < argc) {
            q2_quantum = std::stoi(argv[++i]);
        } else if (arg == "--baseline") {
            adaptive_mode = false;
        } else if (arg == "--verbose" || arg == "-v") {
            verbose = true;
        } else if (arg == "--export" && i + 1 < argc) {
            export_file = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: scheduler [options]\n\n"
                      << "Options:\n"
                      << "  --workload N     Number of transactions to generate (default: 176000)\n"
                      << "  --aging N        Anti-starvation aging threshold in ticks (default: 50)\n"
                      << "  --quantum N      Queue 2 Round Robin time quantum (default: 5)\n"
                      << "  --baseline       Run in Standard Baseline mode (no adaptive backoff)\n"
                      << "  --verbose, -v    Print per-tick progress every 500 ticks\n"
                      << "  --export FILE    Export state transitions to JSON file\n"
                      << "  --help, -h       Show this help message\n";
            return 0;
        }
    }

    // Build and run
    SchedulerEngine engine(workload_size, aging_threshold, q2_quantum, adaptive_mode, verbose);
    engine.run();
    engine.print_report();

    // Export if requested
    if (!export_file.empty()) {
        std::cout << "\nExporting transitions to " << export_file << "...\n";
        if (engine.export_transitions(export_file, 10000)) {
            std::cout << "  Export successful.\n";
        } else {
            std::cerr << "  [ERROR] Failed to write export file.\n";
            return 1;
        }
    }

    return 0;
}
