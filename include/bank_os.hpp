#ifndef BANK_OS_BANK_OS_HPP
#define BANK_OS_BANK_OS_HPP

#include <vector>
#include <deque>
#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <algorithm>

#include "pcb.hpp"
#include "workload_generator.hpp"
#include "mlfq_scheduler.hpp"
#include "lock_manager.hpp"
#include "middleware.hpp"
#include "mysql_storage.hpp"
#include "ml/contention_predictor.hpp"
#include "ml/proactive_quota_manager.hpp"
#include "json.hpp"

class BankOSSimulator {
private:
    std::vector<std::shared_ptr<PCB>> workload_;
    size_t workload_cursor_ = 0;

    MLFQScheduler scheduler_;
    LockManager lock_manager_;
    MDACCMiddleware middleware_;
    StorageEngine storage_;
    ContentionPredictor predictor_;
    ProactiveQuotaManager quota_manager_;

    // Processes waiting for locks or in backoff delay
    std::vector<std::shared_ptr<PCB>> waiting_pool_;

    // Terminated processes
    std::vector<std::shared_ptr<PCB>> completed_pool_;

    // Global simulation state
    std::atomic<int> current_tick_{0};
    std::atomic<bool> is_paused_{false};
    std::atomic<int> tick_delay_us_{1000}; // Execution delay per tick in microseconds

    // Rolling TPS (Transactions per second) calculation
    std::deque<std::pair<std::chrono::steady_clock::time_point, int>> commit_history_;
    double current_tps_ = 0.0;
    std::chrono::steady_clock::time_point last_tps_calc_time_;

    mutable std::mutex sim_mutex_;

public:
    explicit BankOSSimulator(size_t workload_size = 176000, bool adaptive_mode = true, int aging_threshold = 50)
        : scheduler_(aging_threshold, 5),
          lock_manager_(20),
          middleware_(adaptive_mode, 100),
          last_tps_calc_time_(std::chrono::steady_clock::now()) {
        
        // Generate workload
        workload_ = WorkloadGenerator::generate_workload(workload_size, 0, 8000, 42);
    }

    void load_custom_workload(const std::vector<std::shared_ptr<PCB>>& custom_workload) {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        workload_ = custom_workload;
        workload_cursor_ = 0;
        waiting_pool_.clear();
        completed_pool_.clear();
        current_tick_ = 0;
        lock_manager_.clear();
        scheduler_.clear_running_process();
    }

    void set_adaptive_mode(bool adaptive) {
        middleware_.set_adaptive_mode(adaptive);
    }

    bool is_adaptive_mode() const {
        return middleware_.is_adaptive_mode();
    }

    void set_aging_threshold(int x) {
        scheduler_.set_aging_threshold(x);
    }

    void set_paused(bool paused) {
        is_paused_ = paused;
    }

    bool is_paused() const {
        return is_paused_;
    }

    void set_tick_delay_us(int us) {
        tick_delay_us_ = std::max(0, us);
    }

    int get_tick_delay_us() const {
        return tick_delay_us_;
    }

    int get_current_tick() const {
        return current_tick_;
    }

    MLFQScheduler& get_scheduler() { return scheduler_; }
    LockManager& get_lock_manager() { return lock_manager_; }
    MDACCMiddleware& get_middleware() { return middleware_; }
    StorageEngine& get_storage() { return storage_; }
    ContentionPredictor& get_predictor() { return predictor_; }
    ProactiveQuotaManager& get_quota_manager() { return quota_manager_; }

    // Execute a single simulation tick step
    void step_tick() {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        int tick = ++current_tick_;

        // 1. Advance Scheduler Wait Timers
        scheduler_.on_tick_advance();

        // 2. Telemetry and ML Predictor Forward Pass
        TelemetrySnapshot snap = middleware_.get_telemetry_snapshot();
        predictor_.ingest_features(
            static_cast<float>(snap.conflict_rate),
            static_cast<float>(snap.abort_rate),
            static_cast<float>(std::min(1.0, snap.queue_length / 50.0)),
            static_cast<float>(snap.retry_intensity),
            static_cast<float>(std::min(1.0, snap.arrival_velocity / 30.0)),
            static_cast<float>(snap.batch_ratio)
        );
        float predicted_c = predictor_.predict_contention();
        TrafficRegime regime = predictor_.classify_regime();

        // Proactively compress batch queue quantum or raise priority quota if needed
        QuotaAllocation alloc = quota_manager_.evaluate_proactive_quota(predicted_c, regime);
        scheduler_.set_q2_quantum(alloc.q2_batch_quantum);

        // 3. Anti-Starvation Aging Engine: Promote Q2 jobs that waited >= threshold
        scheduler_.promote_aged_processes(tick);

        // 4. Ingest newly arriving transactions from workload
        while (workload_cursor_ < workload_.size() && workload_[workload_cursor_]->arrival_time <= tick) {
            auto proc = workload_[workload_cursor_++];
            middleware_.record_arrival(proc->type);
            scheduler_.add_process(proc, tick);
        }

        // 5. Process Waiting Pool (Lock backoffs & timeouts)
        process_waiting_pool(tick);

        // 6. Check MLFQ Preemption (SRTF preemption or Q2 quantum expiry)
        scheduler_.should_preempt();

        // 7. Dispatch next process from MLFQ
        std::shared_ptr<PCB> running = scheduler_.schedule_next(tick);

        // 8. CPU Execution & Lock Acquisition
        if (running) {
            execute_running_process(running, tick);
        }

        // 9. Update Telemetry Queue Length & Advance Middleware Tick
        int active_ql = static_cast<int>(scheduler_.get_q1_size() + scheduler_.get_q2_size() + waiting_pool_.size());
        middleware_.update_queue_length(active_ql);
        middleware_.advance_tick();

        // 10. Update Rolling TPS Calculation
        update_tps();
    }

private:
    void process_waiting_pool(int tick) {
        auto it = waiting_pool_.begin();
        while (it != waiting_pool_.end()) {
            auto proc = *it;

            // Check if process is still serving backoff delay
            if (proc->backoff_remaining > 0) {
                proc->backoff_remaining--;
                ++it;
                continue;
            }

            // Check if process timed out in waiting state (Tw)
            bool timed_out = lock_manager_.tick_wait_and_check_timeout(proc->pid);
            if (timed_out) {
                proc->abort_count++;
                middleware_.record_abort();
                
                // If maximum retries exceeded or baseline mode abort storm
                if (proc->retry_count >= 5 || !middleware_.is_adaptive_mode()) {
                    // Abort permanently as failed
                    proc->state = ProcessState::TERMINATED_FAILED;
                    proc->failure_reason = "Lock wait timeout (Tw=" + std::to_string(lock_manager_.get_wait_timeout()) + " ticks) exceeded";
                    proc->completion_time = tick;
                    lock_manager_.release_locks(proc->pid);
                    completed_pool_.push_back(proc);
                    scheduler_.log_transition(tick, proc->pid, proc->type, ProcessState::WAITING, ProcessState::TERMINATED_FAILED, proc->failure_reason);
                    it = waiting_pool_.erase(it);
                    continue;
                } else {
                    // Re-enqueue with exponential backoff
                    proc->retry_count++;
                    proc->backoff_remaining = middleware_.calculate_backoff(proc->pid, proc->retry_count);
                    lock_manager_.reset_wait_timer(proc->pid);
                    scheduler_.log_transition(tick, proc->pid, proc->type, ProcessState::WAITING, ProcessState::WAITING, 
                        "Retry " + std::to_string(proc->retry_count) + " with backoff " + std::to_string(proc->backoff_remaining) + " ticks");
                    ++it;
                    continue;
                }
            }

            // Try to acquire locks in strict monotonic order
            middleware_.record_attempt();
            int conflict_acc = 0, conflict_pid = 0;
            bool acquired = lock_manager_.try_acquire_locks(proc->pid, proc->required_locks, conflict_acc, conflict_pid);

            if (acquired) {
                // Locks acquired! Transition to READY and return to Scheduler
                proc->state = ProcessState::READY;
                scheduler_.add_process(proc, tick);
                scheduler_.log_transition(tick, proc->pid, proc->type, ProcessState::WAITING, ProcessState::READY, "Acquired required locks; returned to queue");
                it = waiting_pool_.erase(it);
            } else {
                // Lock still unavailable
                middleware_.record_conflict();
                ++it;
            }
        }
    }

    void execute_running_process(std::shared_ptr<PCB> proc, int tick) {
        // Ensure locks are held
        int conflict_acc = 0, conflict_pid = 0;
        middleware_.record_attempt();
        bool acquired = lock_manager_.try_acquire_locks(proc->pid, proc->required_locks, conflict_acc, conflict_pid);

        if (!acquired) {
            // Lock conflict! Transition to WAITING
            middleware_.record_conflict();
            proc->state = ProcessState::WAITING;
            proc->retry_count++;
            proc->backoff_remaining = middleware_.calculate_backoff(proc->pid, proc->retry_count);
            waiting_pool_.push_back(proc);
            scheduler_.clear_running_process();

            std::string reason = "Lock conflict on Account " + std::to_string(conflict_acc) + " held by PID " + std::to_string(conflict_pid);
            if (proc->backoff_remaining > 0) {
                reason += "; backoff " + std::to_string(proc->backoff_remaining) + " ticks";
            }
            scheduler_.log_transition(tick, proc->pid, proc->type, ProcessState::RUNNING, ProcessState::WAITING, reason);
            return;
        }

        // Execute 1 CPU cycle
        proc->burst_time--;

        // Check if execution is finished
        if (proc->burst_time <= 0) {
            // Process completed CPU execution: Execute ACID transfer
            bool success = storage_.execute_transfer(*proc, tick);

            if (success) {
                middleware_.record_commit();
                record_commit_tps(tick);
                scheduler_.log_transition(tick, proc->pid, proc->type, ProcessState::RUNNING, ProcessState::TERMINATED, "Committed transfer successfully");
            } else {
                middleware_.record_abort();
                scheduler_.log_transition(tick, proc->pid, proc->type, ProcessState::RUNNING, ProcessState::TERMINATED_FAILED, proc->failure_reason);
            }

            // Release all held locks upon completion
            lock_manager_.release_locks(proc->pid);
            completed_pool_.push_back(proc);
            scheduler_.clear_running_process();
        }
    }

    void record_commit_tps(int tick) {
        (void)tick;
        auto now = std::chrono::steady_clock::now();
        commit_history_.push_back({now, 1});
    }

    void update_tps() {
        auto now = std::chrono::steady_clock::now();
        while (!commit_history_.empty()) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - commit_history_.front().first).count();
            if (elapsed >= 3) {
                commit_history_.pop_front();
            } else {
                break;
            }
        }
        current_tps_ = static_cast<double>(commit_history_.size()) / 3.0;
        if (current_tps_ < 0.1 && !commit_history_.empty()) {
            current_tps_ = static_cast<double>(commit_history_.size());
        }
    }

public:
    json get_status_json() const {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        json j = json::object();
        j["current_tick"] = current_tick_.load();
        j["is_paused"] = is_paused_.load();
        j["tick_delay_us"] = tick_delay_us_.load();
        j["queue1_size"] = scheduler_.get_q1_size();
        j["queue2_size"] = scheduler_.get_q2_size();
        j["waiting_size"] = waiting_pool_.size();
        j["completed_size"] = completed_pool_.size();
        j["workload_total"] = workload_.size();
        j["workload_remaining"] = (workload_.size() > workload_cursor_) ? (workload_.size() - workload_cursor_) : 0;
        j["total_commits"] = storage_.get_total_commits();
        j["total_rollbacks"] = storage_.get_total_rollbacks();
        j["tps"] = current_tps_;
        j["active_locks"] = lock_manager_.get_active_locks_count();
        j["comparison_mode"] = middleware_.is_adaptive_mode() ? "ADAPTIVE_MDACC" : "STANDARD_BASELINE";
        return j;
    }

    json get_state_json() const {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        json j = json::object();

        // Status summary
        j["current_tick"] = current_tick_.load();
        j["is_paused"] = is_paused_.load();
        j["tick_delay_us"] = tick_delay_us_.load();
        j["tps"] = current_tps_;
        j["comparison_mode"] = middleware_.is_adaptive_mode() ? "ADAPTIVE_MDACC" : "STANDARD_BASELINE";

        // Running process
        auto running = scheduler_.get_running_process();
        if (running) {
            j["running_process"] = running->to_json();
        } else {
            j["running_process"] = nullptr;
        }

        // Queues PIDs
        json q1_arr = json::array();
        for (int pid : scheduler_.get_q1_pids()) q1_arr.push_back(pid);
        j["queue1_pids"] = q1_arr;

        json q2_arr = json::array();
        for (int pid : scheduler_.get_q2_pids()) q2_arr.push_back(pid);
        j["queue2_pids"] = q2_arr;

        json wait_arr = json::array();
        for (const auto& p : waiting_pool_) wait_arr.push_back(p->pid);
        j["waiting_pids"] = wait_arr;

        // Telemetry snapshot
        TelemetrySnapshot snap = middleware_.get_telemetry_snapshot();
        j["telemetry"] = snap.to_json();

        // Forecast & Regime
        j["forecasted_contention"] = const_cast<ContentionPredictor&>(predictor_).predict_contention();
        j["traffic_regime"] = to_string(predictor_.classify_regime());
        j["ml_inference_latency_ns"] = predictor_.get_last_latency_ns();

        // Proactive Quota
        QuotaAllocation alloc = quota_manager_.get_current_quota();
        json quota_j = json::object();
        quota_j["q1_priority_quota"] = alloc.q1_priority_quota;
        quota_j["q2_batch_quantum"] = alloc.q2_batch_quantum;
        quota_j["is_compressed"] = alloc.is_compressed;
        j["proactive_quota"] = quota_j;

        // Storage counters
        j["total_commits"] = storage_.get_total_commits();
        j["total_rollbacks"] = storage_.get_total_rollbacks();

        // Balances sample
        json bal_j = json::object();
        for (const auto& kv : storage_.get_all_balances()) {
            bal_j[std::to_string(kv.first)] = kv.second;
        }
        j["balances"] = bal_j;

        // Recent active PCBs (up to 30)
        json active_pcbs = json::array();
        if (running) active_pcbs.push_back(running->to_json());
        for (const auto& p : waiting_pool_) {
            if (active_pcbs.size() >= 30) break;
            active_pcbs.push_back(p->to_json());
        }
        j["active_pcbs"] = active_pcbs;

        return j;
    }

    json get_queues_json() const {
        std::lock_guard<std::mutex> lock(sim_mutex_);
        json j = json::object();
        
        auto running = scheduler_.get_running_process();
        j["running"] = running ? running->to_json() : nullptr;

        json q1 = json::array();
        for (int pid : scheduler_.get_q1_pids()) q1.push_back(pid);
        j["queue1"] = q1;

        json q2 = json::array();
        for (int pid : scheduler_.get_q2_pids()) q2.push_back(pid);
        j["queue2"] = q2;

        json waiting = json::array();
        for (const auto& p : waiting_pool_) waiting.push_back(p->to_json());
        j["waiting"] = waiting;

        return j;
    }

    json get_transitions_json(size_t limit = 100) const {
        std::vector<StateTransitionRecord> recs = scheduler_.get_transitions(limit);
        json arr = json::array();
        for (const auto& r : recs) {
            arr.push_back(r.to_json());
        }
        return arr;
    }

    bool export_transitions(const std::string& filename, size_t max_records = 0) const {
        return scheduler_.export_transitions_to_json(filename, max_records);
    }
};

#endif // BANK_OS_BANK_OS_HPP
