#ifndef BANK_OS_PCB_HPP
#define BANK_OS_PCB_HPP

#include <string>
#include <vector>
#include "json.hpp"

enum class TransactionType {
    UPI,
    ATM,
    SALARY
};

inline std::string to_string(TransactionType type) {
    switch (type) {
        case TransactionType::UPI: return "UPI";
        case TransactionType::ATM: return "ATM";
        case TransactionType::SALARY: return "SALARY";
    }
    return "UNKNOWN";
}

inline TransactionType transaction_type_from_string(const std::string& str) {
    if (str == "UPI") return TransactionType::UPI;
    if (str == "ATM") return TransactionType::ATM;
    if (str == "SALARY") return TransactionType::SALARY;
    return TransactionType::UPI;
}

enum class ProcessState {
    NEW,
    READY,
    RUNNING,
    WAITING,
    TERMINATED,
    TERMINATED_FAILED
};

inline std::string to_string(ProcessState state) {
    switch (state) {
        case ProcessState::NEW: return "NEW";
        case ProcessState::READY: return "READY";
        case ProcessState::RUNNING: return "RUNNING";
        case ProcessState::WAITING: return "WAITING";
        case ProcessState::TERMINATED: return "TERMINATED";
        case ProcessState::TERMINATED_FAILED: return "TERMINATED_FAILED";
    }
    return "UNKNOWN";
}

inline ProcessState process_state_from_string(const std::string& str) {
    if (str == "NEW") return ProcessState::NEW;
    if (str == "READY") return ProcessState::READY;
    if (str == "RUNNING") return ProcessState::RUNNING;
    if (str == "WAITING") return ProcessState::WAITING;
    if (str == "TERMINATED") return ProcessState::TERMINATED;
    if (str == "TERMINATED_FAILED") return ProcessState::TERMINATED_FAILED;
    return ProcessState::NEW;
}

struct PCB {
    int pid = 0;
    TransactionType type = TransactionType::UPI;
    ProcessState state = ProcessState::NEW;

    int arrival_time = 0;
    int burst_time = 0;          // Remaining CPU cycles/ticks
    int total_burst_time = 0;    // Initial burst cycles
    
    std::vector<int> required_locks; // Account IDs to acquire
    int from_account = 0;
    int to_account = 0;
    double amount = 0.0;
    std::string failure_reason = "";

    int wait_time = 0;
    int queue2_ticks = 0;        // Duration spent in Q2
    int current_queue = 1;       // 1 (SRTF) or 2 (Round Robin)
    bool aged_promoted = false;  // Promoted to Q1 via aging

    int start_time = -1;
    int completion_time = -1;

    int retry_count = 0;
    int backoff_remaining = 0;   // Ticks remaining in backoff delay
    int abort_count = 0;

    // Helper constructor
    PCB() = default;

    PCB(int p, TransactionType t, int arrival, int burst, int from_acc, int to_acc, double amt)
        : pid(p), type(t), state(ProcessState::NEW), arrival_time(arrival),
          burst_time(burst), total_burst_time(burst),
          from_account(from_acc), to_account(to_acc), amount(amt),
          current_queue((t == TransactionType::SALARY) ? 2 : 1) {}

    json to_json() const {
        json j = json::object();
        j["pid"] = pid;
        j["type"] = to_string(type);
        j["state"] = to_string(state);
        j["arrival_time"] = arrival_time;
        j["burst_time"] = burst_time;
        j["total_burst_time"] = total_burst_time;
        
        json locks = json::array();
        for (int acc : required_locks) {
            locks.push_back(acc);
        }
        j["required_locks"] = locks;

        j["from_account"] = from_account;
        j["to_account"] = to_account;
        j["amount"] = amount;
        j["failure_reason"] = failure_reason;

        j["wait_time"] = wait_time;
        j["queue2_ticks"] = queue2_ticks;
        j["current_queue"] = current_queue;
        j["aged_promoted"] = aged_promoted;

        j["start_time"] = start_time;
        j["completion_time"] = completion_time;

        j["retry_count"] = retry_count;
        j["backoff_remaining"] = backoff_remaining;
        j["abort_count"] = abort_count;

        return j;
    }

    static PCB from_json(const json& j) {
        PCB pcb;
        if (j.contains("pid")) pcb.pid = j["pid"].get<int>();
        if (j.contains("type")) pcb.type = transaction_type_from_string(j["type"].get<std::string>());
        if (j.contains("state")) pcb.state = process_state_from_string(j["state"].get<std::string>());
        if (j.contains("arrival_time")) pcb.arrival_time = j["arrival_time"].get<int>();
        if (j.contains("burst_time")) pcb.burst_time = j["burst_time"].get<int>();
        if (j.contains("total_burst_time")) pcb.total_burst_time = j["total_burst_time"].get<int>();
        if (j.contains("from_account")) pcb.from_account = j["from_account"].get<int>();
        if (j.contains("to_account")) pcb.to_account = j["to_account"].get<int>();
        if (j.contains("amount")) pcb.amount = j["amount"].get<double>();
        if (j.contains("failure_reason")) pcb.failure_reason = j["failure_reason"].get<std::string>();
        if (j.contains("wait_time")) pcb.wait_time = j["wait_time"].get<int>();
        if (j.contains("queue2_ticks")) pcb.queue2_ticks = j["queue2_ticks"].get<int>();
        if (j.contains("current_queue")) pcb.current_queue = j["current_queue"].get<int>();
        if (j.contains("aged_promoted")) pcb.aged_promoted = j["aged_promoted"].get<bool>();
        if (j.contains("start_time")) pcb.start_time = j["start_time"].get<int>();
        if (j.contains("completion_time")) pcb.completion_time = j["completion_time"].get<int>();
        if (j.contains("retry_count")) pcb.retry_count = j["retry_count"].get<int>();
        if (j.contains("backoff_remaining")) pcb.backoff_remaining = j["backoff_remaining"].get<int>();
        if (j.contains("abort_count")) pcb.abort_count = j["abort_count"].get<int>();

        if (j.contains("required_locks") && j["required_locks"].is_array()) {
            for (size_t i = 0; i < j["required_locks"].size(); ++i) {
                pcb.required_locks.push_back(j["required_locks"][i].get<int>());
            }
        }
        return pcb;
    }
};

#endif // BANK_OS_PCB_HPP
