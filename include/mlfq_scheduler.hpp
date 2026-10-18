#ifndef BANK_OS_MLFQ_SCHEDULER_HPP
#define BANK_OS_MLFQ_SCHEDULER_HPP

#include <vector>
#include <deque>
#include <memory>
#include <algorithm>
#include <mutex>
#include <fstream>
#include "pcb.hpp"
#include "json.hpp"

struct StateTransitionRecord {
    int tick;
    int pid;
    std::string type;
    std::string from_state;
    std::string to_state;
    std::string reason;

    json to_json() const {
        json j = json::object();
        j["tick"] = tick;
        j["pid"] = pid;
        j["type"] = type;
        j["from_state"] = from_state;
        j["to_state"] = to_state;
        j["reason"] = reason;
        return j;
    }
};

class MLFQScheduler {
private:
    std::vector<std::shared_ptr<PCB>> queue1_; // Queue 1: High Priority (SRTF)
    std::deque<std::shared_ptr<PCB>> queue2_;  // Queue 2: Low Priority (Round Robin)
    std::shared_ptr<PCB> running_process_ = nullptr;

    int q2_quantum_ = 5;         // Default time quantum for Queue 2
    int current_quantum_used_ = 0; // Cycles executed in current quantum
    int aging_threshold_ = 50;   // Default X = 50 ticks in Queue 2
    
    std::vector<StateTransitionRecord> transition_history_;
    mutable std::mutex scheduler_mutex_;

public:
    explicit MLFQScheduler(int aging_threshold = 50, int q2_quantum = 5)
        : q2_quantum_(q2_quantum), aging_threshold_(aging_threshold) {}

    void set_aging_threshold(int x) {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        aging_threshold_ = x;
    }

    int get_aging_threshold() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        return aging_threshold_;
    }

    void set_q2_quantum(int q) {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        q2_quantum_ = (q > 0 ? q : 1);
    }

    int get_q2_quantum() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        return q2_quantum_;
    }

    void log_transition(int tick, int pid, TransactionType type, ProcessState from_s, ProcessState to_s, const std::string& reason) {
        // Can be called with or without lock held
        StateTransitionRecord rec;
        rec.tick = tick;
        rec.pid = pid;
        rec.type = to_string(type);
        rec.from_state = to_string(from_s);
        rec.to_state = to_string(to_s);
        rec.reason = reason;
        transition_history_.push_back(rec);
    }

    void add_process(std::shared_ptr<PCB> proc, int current_tick) {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        if (!proc) return;

        ProcessState prev_state = proc->state;
        proc->state = ProcessState::READY;

        if (proc->type == TransactionType::SALARY && !proc->aged_promoted) {
            proc->current_queue = 2;
            queue2_.push_back(proc);
            log_transition(current_tick, proc->pid, proc->type, prev_state, ProcessState::READY, "Enqueued in Queue 2 (Salary batch RR)");
        } else {
            proc->current_queue = 1;
            queue1_.push_back(proc);
            log_transition(current_tick, proc->pid, proc->type, prev_state, ProcessState::READY, 
                proc->aged_promoted ? "Enqueued in Queue 1 (Aged promotion)" : "Enqueued in Queue 1 (High priority interactive)");
        }
    }

    void promote_aged_processes(int current_tick) {
        // Anti-Starvation Aging Engine:
        // If a Salary transaction in Queue 2 waits >= X ticks, promote it to Queue 1.
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        auto it = queue2_.begin();
        while (it != queue2_.end()) {
            auto proc = *it;
            if (proc->queue2_ticks >= aging_threshold_ && !proc->aged_promoted) {
                proc->aged_promoted = true;
                proc->current_queue = 1;
                queue1_.push_back(proc);
                it = queue2_.erase(it);
                log_transition(current_tick, proc->pid, proc->type, proc->state, proc->state, 
                    "Aged promotion: wait " + std::to_string(proc->queue2_ticks) + " ticks >= threshold " + std::to_string(aging_threshold_));
            } else {
                ++it;
            }
        }
    }

    // Check if running process should be preempted by a higher priority or shorter job in Queue 1
    bool should_preempt() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        if (!running_process_) return false;
        
        // If running process is from Queue 2 and Queue 1 has any ready job, preempt immediately
        if (running_process_->current_queue == 2 && !queue1_.empty()) {
            return true;
        }

        // If running process is in Queue 1, check if there is a strictly shorter job in Queue 1
        if (running_process_->current_queue == 1 && !queue1_.empty()) {
            for (const auto& p : queue1_) {
                if (p->burst_time < running_process_->burst_time) {
                    return true;
                }
            }
        }

        // Check Round Robin quantum expiration for Queue 2 job
        if (running_process_->current_queue == 2 && current_quantum_used_ >= q2_quantum_) {
            return true;
        }

        return false;
    }

    std::shared_ptr<PCB> schedule_next(int current_tick) {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);

        // 1. Check if running process finished
        if (running_process_ && (running_process_->state == ProcessState::TERMINATED || running_process_->state == ProcessState::TERMINATED_FAILED)) {
            running_process_ = nullptr;
            current_quantum_used_ = 0;
        }

        // 2. Queue 1 Dispatching: Shortest Remaining Time First (SRTF)
        if (!queue1_.empty()) {
            // Find process with minimum burst_time; tie-break by arrival_time, then pid
            auto best_it = queue1_.begin();
            for (auto it = queue1_.begin() + 1; it != queue1_.end(); ++it) {
                if ((*it)->burst_time < (*best_it)->burst_time) {
                    best_it = it;
                } else if ((*it)->burst_time == (*best_it)->burst_time) {
                    if ((*it)->arrival_time < (*best_it)->arrival_time) {
                        best_it = it;
                    } else if ((*it)->arrival_time == (*best_it)->arrival_time && (*it)->pid < (*best_it)->pid) {
                        best_it = it;
                    }
                }
            }

            // Check if we preempt running process or dispatch next
            if (running_process_) {
                if (running_process_->current_queue == 2 || (*best_it)->burst_time < running_process_->burst_time) {
                    // Preempt running process back to its queue
                    auto preempted = running_process_;
                    preempted->state = ProcessState::READY;
                    if (preempted->current_queue == 2) {
                        queue2_.push_front(preempted); // Put back to resume when ready
                        log_transition(current_tick, preempted->pid, preempted->type, ProcessState::RUNNING, ProcessState::READY, 
                            "Preempted by Queue 1 SRTF job");
                    } else {
                        queue1_.push_back(preempted);
                        log_transition(current_tick, preempted->pid, preempted->type, ProcessState::RUNNING, ProcessState::READY, 
                            "Preempted by shorter Queue 1 job");
                    }
                    running_process_ = *best_it;
                    queue1_.erase(best_it);
                    current_quantum_used_ = 0;
                    if (running_process_->start_time < 0) running_process_->start_time = current_tick;
                    ProcessState prev = running_process_->state;
                    running_process_->state = ProcessState::RUNNING;
                    log_transition(current_tick, running_process_->pid, running_process_->type, prev, ProcessState::RUNNING, "Dispatched via Q1 SRTF");
                    return running_process_;
                }
            } else {
                running_process_ = *best_it;
                queue1_.erase(best_it);
                current_quantum_used_ = 0;
                if (running_process_->start_time < 0) running_process_->start_time = current_tick;
                ProcessState prev = running_process_->state;
                running_process_->state = ProcessState::RUNNING;
                log_transition(current_tick, running_process_->pid, running_process_->type, prev, ProcessState::RUNNING, "Dispatched via Q1 SRTF");
                return running_process_;
            }
        }

        // 3. Queue 2 Dispatching: Round Robin with quantum Q
        if (running_process_) {
            if (running_process_->current_queue == 2 && current_quantum_used_ >= q2_quantum_) {
                // Quantum expired: rotate to the back of Queue 2
                auto rotated = running_process_;
                rotated->state = ProcessState::READY;
                queue2_.push_back(rotated);
                log_transition(current_tick, rotated->pid, rotated->type, ProcessState::RUNNING, ProcessState::READY, 
                    "Quantum expired (" + std::to_string(q2_quantum_) + " ticks): rotated to back of Q2");
                running_process_ = nullptr;
                current_quantum_used_ = 0;
            } else {
                // Running process continues
                return running_process_;
            }
        }

        // Dispatch next job from Queue 2 if available
        if (!queue2_.empty()) {
            running_process_ = queue2_.front();
            queue2_.pop_front();
            current_quantum_used_ = 0;
            if (running_process_->start_time < 0) running_process_->start_time = current_tick;
            ProcessState prev = running_process_->state;
            running_process_->state = ProcessState::RUNNING;
            log_transition(current_tick, running_process_->pid, running_process_->type, prev, ProcessState::RUNNING, "Dispatched via Q2 Round Robin");
            return running_process_;
        }

        return running_process_;
    }

    void on_tick_advance() {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        if (running_process_) {
            current_quantum_used_++;
        }

        // Update wait time for processes in queues
        for (auto& p : queue1_) {
            p->wait_time++;
        }
        for (auto& p : queue2_) {
            p->wait_time++;
            p->queue2_ticks++;
        }
    }

    std::shared_ptr<PCB> get_running_process() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        return running_process_;
    }

    void clear_running_process() {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        running_process_ = nullptr;
        current_quantum_used_ = 0;
    }

    std::vector<int> get_q1_pids() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        std::vector<int> pids;
        pids.reserve(queue1_.size());
        for (const auto& p : queue1_) {
            pids.push_back(p->pid);
        }
        return pids;
    }

    std::vector<int> get_q2_pids() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        std::vector<int> pids;
        pids.reserve(queue2_.size());
        for (const auto& p : queue2_) {
            pids.push_back(p->pid);
        }
        return pids;
    }

    size_t get_q1_size() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        return queue1_.size();
    }

    size_t get_q2_size() const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        return queue2_.size();
    }

    std::vector<StateTransitionRecord> get_transitions(size_t max_records = 200) const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        if (transition_history_.size() <= max_records) {
            return transition_history_;
        }
        return std::vector<StateTransitionRecord>(
            transition_history_.end() - max_records,
            transition_history_.end()
        );
    }

    bool export_transitions_to_json(const std::string& filename, size_t max_records = 0) const {
        std::lock_guard<std::mutex> lock(scheduler_mutex_);
        json arr = json::array();
        size_t start_idx = 0;
        if (max_records > 0 && transition_history_.size() > max_records) {
            start_idx = transition_history_.size() - max_records;
        }

        for (size_t i = start_idx; i < transition_history_.size(); ++i) {
            arr.push_back(transition_history_[i].to_json());
        }

        std::ofstream ofs(filename);
        if (!ofs.is_open()) return false;
        ofs << arr.dump(2);
        return true;
    }
};

#endif // BANK_OS_MLFQ_SCHEDULER_HPP
