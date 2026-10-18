#ifndef BANK_OS_LOCK_MANAGER_HPP
#define BANK_OS_LOCK_MANAGER_HPP

#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <algorithm>
#include <iostream>
#include <memory>
#include "pcb.hpp"

class LockManager {
private:
    // Account ID -> PID currently holding lock
    std::map<int, int> lock_table_;
    
    // PID -> Set of Account IDs currently held
    std::map<int, std::set<int>> pid_held_locks_;

    // PID -> Number of ticks spent waiting for locks
    std::map<int, int> waiting_ticks_;

    // Configurable bounded wait timeout (Tw)
    int wait_timeout_ticks_ = 20;

    mutable std::mutex lock_mgr_mutex_;

public:
    explicit LockManager(int wait_timeout_ticks = 20)
        : wait_timeout_ticks_(wait_timeout_ticks) {}

    void set_wait_timeout(int tw) {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        wait_timeout_ticks_ = tw;
    }

    int get_wait_timeout() const {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        return wait_timeout_ticks_;
    }

    // Sort required locks strictly in ascending order (Havender's Principle)
    static std::vector<int> get_monotonic_lock_order(const std::vector<int>& raw_locks) {
        std::set<int> unique_locks(raw_locks.begin(), raw_locks.end());
        std::vector<int> sorted_locks(unique_locks.begin(), unique_locks.end());
        std::sort(sorted_locks.begin(), sorted_locks.end()); // Strictly ascending order
        return sorted_locks;
    }

    // Try-lock all required locks in strict monotonic order.
    // If ANY lock is unavailable, releases any locks acquired in this call
    // to strictly prevent partial lock holding while waiting.
    bool try_acquire_locks(int pid, const std::vector<int>& raw_locks, int& conflicting_account, int& conflicting_pid) {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        
        std::vector<int> sorted_locks = get_monotonic_lock_order(raw_locks);
        std::vector<int> acquired_in_this_call;

        for (int acc_id : sorted_locks) {
            auto it = lock_table_.find(acc_id);
            if (it != lock_table_.end() && it->second != pid) {
                // Conflict! Lock is held by another PID
                conflicting_account = acc_id;
                conflicting_pid = it->second;

                // Roll back any locks acquired during this attempt
                for (int rollback_acc : acquired_in_this_call) {
                    lock_table_.erase(rollback_acc);
                    pid_held_locks_[pid].erase(rollback_acc);
                }
                if (pid_held_locks_[pid].empty()) {
                    pid_held_locks_.erase(pid);
                }
                return false;
            } else {
                // Available or already held by this pid
                if (it == lock_table_.end()) {
                    lock_table_[acc_id] = pid;
                    pid_held_locks_[pid].insert(acc_id);
                    acquired_in_this_call.push_back(acc_id);
                }
            }
        }

        // Successfully acquired all locks
        waiting_ticks_.erase(pid);
        return true;
    }

    // Check if process has timed out while waiting (bounded wait timeout Tw)
    bool tick_wait_and_check_timeout(int pid) {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        int current_wait = ++waiting_ticks_[pid];
        return current_wait >= wait_timeout_ticks_;
    }

    void reset_wait_timer(int pid) {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        waiting_ticks_.erase(pid);
    }

    // Release all locks held by a PID upon completion or preemption/abort
    std::vector<int> release_locks(int pid) {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        std::vector<int> released;
        
        auto it = pid_held_locks_.find(pid);
        if (it != pid_held_locks_.end()) {
            for (int acc_id : it->second) {
                lock_table_.erase(acc_id);
                released.push_back(acc_id);
            }
            pid_held_locks_.erase(it);
        }
        waiting_ticks_.erase(pid);
        return released;
    }

    bool is_locked(int account_id) const {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        return lock_table_.find(account_id) != lock_table_.end();
    }

    int get_lock_holder(int account_id) const {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        auto it = lock_table_.find(account_id);
        if (it != lock_table_.end()) {
            return it->second;
        }
        return -1;
    }

    std::vector<int> get_held_locks(int pid) const {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        std::vector<int> res;
        auto it = pid_held_locks_.find(pid);
        if (it != pid_held_locks_.end()) {
            res.assign(it->second.begin(), it->second.end());
        }
        return res;
    }

    size_t get_active_locks_count() const {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        return lock_table_.size();
    }

    void clear() {
        std::lock_guard<std::mutex> lock(lock_mgr_mutex_);
        lock_table_.clear();
        pid_held_locks_.clear();
        waiting_ticks_.clear();
    }
};

#endif // BANK_OS_LOCK_MANAGER_HPP
