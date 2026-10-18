#ifndef BANK_OS_DEADLOCK_PREVENTION_HPP
#define BANK_OS_DEADLOCK_PREVENTION_HPP

#include <vector>
#include <map>
#include <set>
#include <queue>
#include <string>
#include <memory>
#include <mutex>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <atomic>

#include "pcb.hpp"
#include "json.hpp"

/**
 * ─────────────────────────────────────────────────────────────
 * Deadlock Prevention, Avoidance, and Detection Subsystem
 *
 * Implements the theoretical foundations of operating system
 * concurrency and resource allocation:
 *
 * 1. HAVENDER'S PRINCIPLE (Prevention - Circular Wait Elimination):
 *    Strict monotonic ascending order of resource IDs ensures that
 *    an allocation cycle is mathematically impossible.
 *
 * 2. ALL-OR-NOTHING / TWO-PHASE (Prevention - Hold & Wait Elimination):
 *    Transactions must acquire all required account locks simultaneously;
 *    if any lock is unavailable, partial locks are rolled back.
 *
 * 3. BOUNDED WAIT PREEMPTION (Prevention - No Preemption Elimination):
 *    If a process waits for locks exceeding Tw ticks, it is preempted
 *    and rolled back, releasing all held resources.
 *
 * 4. DIJKSTRA'S BANKER'S ALGORITHM (Deadlock Avoidance):
 *    Evaluates whether granting a multi-account lock allocation keeps
 *    the banking system in a Safe State (at least one valid execution sequence exists).
 *
 * 5. WAIT-FOR GRAPH (WFG) & CYCLE DETECTION (Detection & Recovery):
 *    Maintains a directed resource dependency graph G = (V, E) where
 *    Pi -> Pj denotes Pi waiting on a lock held by Pj. Employs DFS / Tarjan's
 *    algorithm to find directed cycles and selects cost-optimal victims.
 * ─────────────────────────────────────────────────────────────
 */

// ─────────────────────────────────────────────────────────────
// 1. Havender's Total Resource Ordering (Deadlock Prevention)
// ─────────────────────────────────────────────────────────────

class HavenderProtocol {
public:
    // Sorts raw account IDs in strictly ascending order and removes duplicates
    static std::vector<int> enforce_monotonic_order(const std::vector<int>& raw_locks) {
        std::set<int> unique_ids(raw_locks.begin(), raw_locks.end());
        std::vector<int> ordered(unique_ids.begin(), unique_ids.end());
        std::sort(ordered.begin(), ordered.end());
        return ordered;
    }

    // Verifies whether a given sequence of lock requests strictly satisfies Havender's rule
    static bool is_monotonic(const std::vector<int>& locks) {
        for (size_t i = 1; i < locks.size(); ++i) {
            if (locks[i] <= locks[i - 1]) return false;
        }
        return true;
    }
};

// ─────────────────────────────────────────────────────────────
// 2. Wait-For Graph (WFG) & Cycle Detection (Deadlock Detection)
// ─────────────────────────────────────────────────────────────

struct DeadlockCycle {
    std::vector<int> cycle_pids; // e.g. [101, 104, 101]
    int detected_tick = 0;
    std::string description;

    json to_json() const {
        json j = json::object();
        json pids = json::array();
        for (int p : cycle_pids) {
            pids.push_back(p);
        }
        j["cycle_pids"] = pids;
        j["detected_tick"] = detected_tick;
        j["description"] = description;
        return j;
    }
};

class WaitForGraph {
private:
    // Directed graph: Waiter PID -> Set of Holder PIDs
    std::map<int, std::set<int>> adj_;
    mutable std::mutex wfg_mutex_;

    long long total_cycle_checks_ = 0;
    long long total_cycles_detected_ = 0;

public:
    void clear() {
        std::lock_guard<std::mutex> lock(wfg_mutex_);
        adj_.clear();
    }

    void add_wait_edge(int waiter_pid, int holder_pid) {
        std::lock_guard<std::mutex> lock(wfg_mutex_);
        if (waiter_pid != holder_pid && waiter_pid > 0 && holder_pid > 0) {
            adj_[waiter_pid].insert(holder_pid);
        }
    }

    void remove_process(int pid) {
        std::lock_guard<std::mutex> lock(wfg_mutex_);
        adj_.erase(pid);
        for (auto& pair : adj_) {
            pair.second.erase(pid);
        }
    }

    // DFS-based cycle detection; returns true if any directed cycle exists
    bool detect_cycle(DeadlockCycle& out_cycle, int current_tick = 0) {
        std::lock_guard<std::mutex> lock(wfg_mutex_);
        total_cycle_checks_++;

        std::map<int, int> visited; // 0 = unvisited, 1 = visiting (gray), 2 = done (black)
        std::vector<int> path;

        for (const auto& node : adj_) {
            int start_pid = node.first;
            if (visited[start_pid] == 0) {
                path.clear();
                if (dfs_find_cycle(start_pid, visited, path, out_cycle)) {
                    total_cycles_detected_++;
                    out_cycle.detected_tick = current_tick;
                    std::ostringstream ss;
                    ss << "Circular Wait detected: ";
                    for (size_t i = 0; i < out_cycle.cycle_pids.size(); ++i) {
                        ss << "PID " << out_cycle.cycle_pids[i];
                        if (i + 1 < out_cycle.cycle_pids.size()) ss << " -> ";
                    }
                    out_cycle.description = ss.str();
                    return true;
                }
            }
        }
        return false;
    }

    size_t get_active_nodes_count() const {
        std::lock_guard<std::mutex> lock(wfg_mutex_);
        return adj_.size();
    }

    long long get_cycle_checks_count() const { return total_cycle_checks_; }
    long long get_cycles_detected_count() const { return total_cycles_detected_; }

private:
    bool dfs_find_cycle(int u, std::map<int, int>& visited, std::vector<int>& path, DeadlockCycle& out_cycle) {
        visited[u] = 1;
        path.push_back(u);

        auto it = adj_.find(u);
        if (it != adj_.end()) {
            for (int v : it->second) {
                if (visited[v] == 1) {
                    // Back-edge found: Cycle!
                    out_cycle.cycle_pids.clear();
                    auto cycle_start = std::find(path.begin(), path.end(), v);
                    if (cycle_start != path.end()) {
                        out_cycle.cycle_pids.assign(cycle_start, path.end());
                        out_cycle.cycle_pids.push_back(v); // Close cycle loop
                    } else {
                        out_cycle.cycle_pids = {u, v, u};
                    }
                    return true;
                }
                if (visited[v] == 0) {
                    if (dfs_find_cycle(v, visited, path, out_cycle)) {
                        return true;
                    }
                }
            }
        }

        path.pop_back();
        visited[u] = 2;
        return false;
    }
};

// ─────────────────────────────────────────────────────────────
// 3. Deadlock Recovery & Cost-Optimal Victim Selector
// ─────────────────────────────────────────────────────────────

struct VictimMetrics {
    int pid = 0;
    int queue_level = 1;
    int burst_remaining = 0;
    int locks_held_count = 0;
    int retry_count = 0;
    double selection_cost = 0.0;
};

class DeadlockRecoveryEngine {
public:
    /**
     * Cost Function for selecting victim process to abort/preempt:
     * Cost(P) = 3.0 * queue_level + 1.5 * burst_remaining - 4.0 * retry_count - 2.0 * locks_held
     * Lower cost = better candidate for preemption (avoids starving older retrying transactions).
     */
    static int select_optimal_victim(const std::vector<int>& cycle_pids,
                                    const std::map<int, std::shared_ptr<PCB>>& process_lookup,
                                    const std::map<int, std::set<int>>& pid_held_locks) {
        if (cycle_pids.empty()) return -1;

        int best_victim_pid = -1;
        double min_cost = 1e9;

        for (int pid : cycle_pids) {
            auto it = process_lookup.find(pid);
            if (it == process_lookup.end() || !it->second) continue;

            const auto& proc = it->second;
            int held_count = 0;
            auto h_it = pid_held_locks.find(pid);
            if (h_it != pid_held_locks.end()) held_count = static_cast<int>(h_it->second.size());

            // Cost formula
            double cost = (3.0 * proc->current_queue)
                        + (1.5 * proc->burst_time)
                        - (4.0 * proc->retry_count)
                        - (2.0 * held_count);

            if (cost < min_cost) {
                min_cost = cost;
                best_victim_pid = pid;
            }
        }

        return (best_victim_pid != -1) ? best_victim_pid : cycle_pids.front();
    }
};

// ─────────────────────────────────────────────────────────────
// 4. Dijkstra's Banker's Algorithm (Deadlock Avoidance)
// ─────────────────────────────────────────────────────────────

class BankersAlgorithm {
public:
    struct SystemState {
        std::vector<int> account_ids;                  // Resource types (m)
        std::vector<int> available;                    // Available instances per account (1 each in banking)
        std::vector<int> pids;                         // Active process IDs (n)
        std::vector<std::vector<int>> max_claim;       // Max need matrix [n x m]
        std::vector<std::vector<int>> allocation;      // Current allocation [n x m]
        std::vector<std::vector<int>> need;            // Remaining need [n x m]
    };

    // Evaluates whether current system state is safe, returning safe sequence if valid
    static bool is_safe_state(const SystemState& state, std::vector<int>& safe_sequence) {
        size_t n = state.pids.size();
        size_t m = state.account_ids.size();
        if (n == 0 || m == 0) return true;

        std::vector<int> work = state.available;
        std::vector<bool> finish(n, false);
        safe_sequence.clear();

        size_t count = 0;
        while (count < n) {
            bool found = false;
            for (size_t p = 0; p < n; ++p) {
                if (!finish[p]) {
                    // Check if need <= work for all resource types
                    bool can_allocate = true;
                    for (size_t r = 0; r < m; ++r) {
                        if (state.need[p][r] > work[r]) {
                            can_allocate = false;
                            break;
                        }
                    }

                    if (can_allocate) {
                        for (size_t r = 0; r < m; ++r) {
                            work[r] += state.allocation[p][r];
                        }
                        safe_sequence.push_back(state.pids[p]);
                        finish[p] = true;
                        found = true;
                        count++;
                    }
                }
            }

            // If no process could be completed in this pass, state is unsafe!
            if (!found) {
                return false;
            }
        }

        return true;
    }

    // Deadlock Avoidance check: tests whether granting request maintains safe state
    static bool can_grant_request(const SystemState& state, size_t process_idx,
                                 const std::vector<int>& request,
                                 std::vector<int>& resulting_safe_seq) {
        size_t m = state.account_ids.size();

        // 1. Check if request <= need
        for (size_t r = 0; r < m; ++r) {
            if (request[r] > state.need[process_idx][r]) return false;
        }

        // 2. Check if request <= available
        for (size_t r = 0; r < m; ++r) {
            if (request[r] > state.available[r]) return false;
        }

        // 3. Pretend to allocate and run safety check
        SystemState test_state = state;
        for (size_t r = 0; r < m; ++r) {
            test_state.available[r] -= request[r];
            test_state.allocation[process_idx][r] += request[r];
            test_state.need[process_idx][r] -= request[r];
        }

        return is_safe_state(test_state, resulting_safe_seq);
    }
};

// ─────────────────────────────────────────────────────────────
// 5. Coffman Conditions Comprehensive Auditor
// ─────────────────────────────────────────────────────────────

struct CoffmanAuditReport {
    bool mutual_exclusion_enforced = true;
    bool hold_and_wait_eliminated = true;
    bool no_preemption_eliminated = true;
    bool circular_wait_eliminated = true;

    long long total_transactions_checked = 0;
    long long havender_monotonic_passes = 0;
    long long havender_monotonic_fails = 0;
    long long all_or_nothing_rollbacks = 0;
    long long bounded_timeout_preemptions = 0;
    long long wfg_cycle_checks = 0;
    long long wfg_cycles_found = 0;

    bool is_provably_deadlock_free() const {
        return hold_and_wait_eliminated && circular_wait_eliminated && (wfg_cycles_found == 0);
    }

    json to_json() const {
        json j = json::object();
        j["mutual_exclusion_enforced"] = mutual_exclusion_enforced;
        j["hold_and_wait_eliminated"] = hold_and_wait_eliminated;
        j["no_preemption_eliminated"] = no_preemption_eliminated;
        j["circular_wait_eliminated"] = circular_wait_eliminated;
        j["total_transactions_checked"] = total_transactions_checked;
        j["havender_monotonic_passes"] = havender_monotonic_passes;
        j["havender_monotonic_fails"] = havender_monotonic_fails;
        j["all_or_nothing_rollbacks"] = all_or_nothing_rollbacks;
        j["bounded_timeout_preemptions"] = bounded_timeout_preemptions;
        j["wfg_cycle_checks"] = wfg_cycle_checks;
        j["wfg_cycles_found"] = wfg_cycles_found;
        j["is_provably_deadlock_free"] = is_provably_deadlock_free();
        return j;
    }
};

#endif // BANK_OS_DEADLOCK_PREVENTION_HPP
