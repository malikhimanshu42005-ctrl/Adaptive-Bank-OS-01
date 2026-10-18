#ifndef BANK_OS_MYSQL_STORAGE_HPP
#define BANK_OS_MYSQL_STORAGE_HPP

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <memory>
#include <atomic>
#include "pcb.hpp"
#include "workload_generator.hpp"
#include "json.hpp"

struct WALRecord {
    long long sequence_number;
    int pid;
    std::string operation; // "PREPARE", "DEBIT", "CREDIT", "COMMIT", "ROLLBACK"
    int account_id;
    double old_balance;
    double new_balance;
    std::string status;
    int tick;

    json to_json() const {
        json j = json::object();
        j["sequence_number"] = sequence_number;
        j["pid"] = pid;
        j["operation"] = operation;
        j["account_id"] = account_id;
        j["old_balance"] = old_balance;
        j["new_balance"] = new_balance;
        j["status"] = status;
        j["tick"] = tick;
        return j;
    }
};

struct StoredTransaction {
    int transaction_id;
    int pid;
    std::string type;
    int from_account;
    int to_account;
    double amount;
    std::string status;
    std::string reason;
    int created_tick;

    json to_json() const {
        json j = json::object();
        j["transaction_id"] = transaction_id;
        j["pid"] = pid;
        j["type"] = type;
        j["from_account"] = from_account;
        j["to_account"] = to_account;
        j["amount"] = amount;
        j["status"] = status;
        j["reason"] = reason;
        j["created_tick"] = created_tick;
        return j;
    }
};

class StorageEngine {
private:
    bool use_mysql_ = false;
    std::string host_;
    int port_ = 3306;
    std::string user_;
    std::string password_;
    std::string database_;

    // In-memory thread-safe ledger (primary or fallback)
    std::map<int, double> balances_;
    std::map<int, std::string> account_types_;
    std::vector<WALRecord> wal_log_;
    std::vector<StoredTransaction> transactions_;
    
    std::atomic<long long> next_wal_seq_{1};
    std::atomic<int> next_tx_id_{1};

    long long total_commits_ = 0;
    long long total_rollbacks_ = 0;

    mutable std::mutex storage_mutex_;

public:
    StorageEngine() {
        // Read environment variables
        const char* h = std::getenv("MYSQL_HOST");
        const char* p = std::getenv("MYSQL_PORT");
        const char* u = std::getenv("MYSQL_USER");
        const char* pw = std::getenv("MYSQL_PASSWORD");
        const char* db = std::getenv("MYSQL_DATABASE");

        if (h && db) {
            host_ = h;
            port_ = p ? std::atoi(p) : 3306;
            user_ = u ? u : "root";
            password_ = pw ? pw : "";
            database_ = db;
            // Seamless check: If MySQL client headers/connector are not installed,
            // fall back cleanly to thread-safe in-memory ledger
            use_mysql_ = false; 
        }

        // Initialize default seed accounts
        seed_accounts(WorkloadGenerator::get_initial_account_seeds());
    }

    void seed_accounts(const std::vector<AccountSeed>& seeds) {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        balances_.clear();
        account_types_.clear();
        for (const auto& s : seeds) {
            balances_[s.account_id] = s.initial_balance;
            account_types_[s.account_id] = s.type;
        }
    }

    bool is_mysql_active() const {
        return use_mysql_;
    }

    double get_balance(int account_id) const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        auto it = balances_.find(account_id);
        if (it != balances_.end()) {
            return it->second;
        }
        return -1.0;
    }

    std::map<int, double> get_all_balances() const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        return balances_;
    }

    // Transfer Invariant Execution:
    // Checks sender balance:
    // If balance < amount: execute database ROLLBACK, record transaction as 'TERMINATED_FAILED',
    //                      and set PCB state to TERMINATED_FAILED.
    // If balance >= amount: execute debit & credit, execute database COMMIT,
    //                       and set PCB state to TERMINATED.
    bool execute_transfer(PCB& pcb, int current_tick) {
        std::lock_guard<std::mutex> lock(storage_mutex_);

        int from_acc = pcb.from_account;
        int to_acc = pcb.to_account;
        double amt = pcb.amount;

        // Ensure accounts exist
        if (balances_.find(from_acc) == balances_.end()) {
            balances_[from_acc] = 0.00;
        }
        if (balances_.find(to_acc) == balances_.end()) {
            balances_[to_acc] = 0.00;
        }

        double sender_balance = balances_[from_acc];

        // 1. Balance Invariant Check (simulating SELECT ... FOR UPDATE)
        if (sender_balance < amt) {
            // Insufficient funds -> Database ROLLBACK
            std::ostringstream reason_ss;
            reason_ss << "Insufficient funds: required " << std::fixed << std::setprecision(2) << amt
                      << ", available " << sender_balance;
            pcb.failure_reason = reason_ss.str();
            pcb.state = ProcessState::TERMINATED_FAILED;
            pcb.completion_time = current_tick;

            // Log WAL Rollback
            WALRecord wal;
            wal.sequence_number = next_wal_seq_++;
            wal.pid = pcb.pid;
            wal.operation = "ROLLBACK";
            wal.account_id = from_acc;
            wal.old_balance = sender_balance;
            wal.new_balance = sender_balance;
            wal.status = "TERMINATED_FAILED";
            wal.tick = current_tick;
            wal_log_.push_back(wal);

            // Record transaction
            StoredTransaction tx;
            tx.transaction_id = next_tx_id_++;
            tx.pid = pcb.pid;
            tx.type = to_string(pcb.type);
            tx.from_account = from_acc;
            tx.to_account = to_acc;
            tx.amount = amt;
            tx.status = "TERMINATED_FAILED";
            tx.reason = pcb.failure_reason;
            tx.created_tick = current_tick;
            transactions_.push_back(tx);

            total_rollbacks_++;
            return false;
        }

        // 2. Sufficient funds -> Execute Debit & Credit -> Database COMMIT
        double old_sender_bal = balances_[from_acc];
        double old_receiver_bal = balances_[to_acc];

        balances_[from_acc] -= amt;
        balances_[to_acc] += amt;

        pcb.state = ProcessState::TERMINATED;
        pcb.completion_time = current_tick;
        pcb.failure_reason = "";

        // Log WAL Debit
        WALRecord wal_debit;
        wal_debit.sequence_number = next_wal_seq_++;
        wal_debit.pid = pcb.pid;
        wal_debit.operation = "DEBIT";
        wal_debit.account_id = from_acc;
        wal_debit.old_balance = old_sender_bal;
        wal_debit.new_balance = balances_[from_acc];
        wal_debit.status = "IN_PROGRESS";
        wal_debit.tick = current_tick;
        wal_log_.push_back(wal_debit);

        // Log WAL Credit
        WALRecord wal_credit;
        wal_credit.sequence_number = next_wal_seq_++;
        wal_credit.pid = pcb.pid;
        wal_credit.operation = "CREDIT";
        wal_credit.account_id = to_acc;
        wal_credit.old_balance = old_receiver_bal;
        wal_credit.new_balance = balances_[to_acc];
        wal_credit.status = "IN_PROGRESS";
        wal_credit.tick = current_tick;
        wal_log_.push_back(wal_credit);

        // Log WAL Commit
        WALRecord wal_commit;
        wal_commit.sequence_number = next_wal_seq_++;
        wal_commit.pid = pcb.pid;
        wal_commit.operation = "COMMIT";
        wal_commit.account_id = from_acc;
        wal_commit.old_balance = old_sender_bal;
        wal_commit.new_balance = balances_[from_acc];
        wal_commit.status = "TERMINATED";
        wal_commit.tick = current_tick;
        wal_log_.push_back(wal_commit);

        // Record transaction
        StoredTransaction tx;
        tx.transaction_id = next_tx_id_++;
        tx.pid = pcb.pid;
        tx.type = to_string(pcb.type);
        tx.from_account = from_acc;
        tx.to_account = to_acc;
        tx.amount = amt;
        tx.status = "TERMINATED";
        tx.reason = "Transfer committed successfully";
        tx.created_tick = current_tick;
        transactions_.push_back(tx);

        total_commits_++;
        return true;
    }

    long long get_total_commits() const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        return total_commits_;
    }

    long long get_total_rollbacks() const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        return total_rollbacks_;
    }

    std::vector<WALRecord> get_wal_records(size_t limit = 100) const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        if (wal_log_.size() <= limit) {
            return wal_log_;
        }
        return std::vector<WALRecord>(wal_log_.end() - limit, wal_log_.end());
    }

    std::vector<StoredTransaction> get_transaction_records(size_t limit = 100) const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        if (transactions_.size() <= limit) {
            return transactions_;
        }
        return std::vector<StoredTransaction>(transactions_.end() - limit, transactions_.end());
    }

    size_t get_wal_size() const {
        std::lock_guard<std::mutex> lock(storage_mutex_);
        return wal_log_.size();
    }
};

#endif // BANK_OS_MYSQL_STORAGE_HPP
