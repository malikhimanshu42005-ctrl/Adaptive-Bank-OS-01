#ifndef BANK_OS_WORKLOAD_GENERATOR_HPP
#define BANK_OS_WORKLOAD_GENERATOR_HPP

#include <vector>
#include <random>
#include <algorithm>
#include <memory>
#include <map>
#include "pcb.hpp"

struct AccountSeed {
    int account_id;
    std::string type; // "RETAIL", "CORPORATE", "ATM_VAULT"
    double initial_balance;
};

class WorkloadGenerator {
public:
    static std::vector<AccountSeed> get_initial_account_seeds() {
        return {
            {1001, "RETAIL", 50000.00},
            {1002, "RETAIL", 50000.00},
            {1003, "RETAIL", 50000.00},
            {1004, "RETAIL", 50000.00},
            {1005, "RETAIL", 300.00},   // Low balance to force insufficient-funds rollbacks
            {1006, "RETAIL", 50000.00},
            {1007, "RETAIL", 50000.00},
            {1008, "RETAIL", 100.00},   // Low balance to force insufficient-funds rollbacks
            {2001, "CORPORATE", 5000000.00},
            {2002, "CORPORATE", 5000000.00},
            {5001, "ATM_VAULT", 1000000.00},
            {5002, "ATM_VAULT", 1000000.00}
        };
    }

    static std::vector<std::shared_ptr<PCB>> generate_workload(
        size_t num_transactions = 176000,
        int min_tick = 0,
        int max_tick = 8000,
        unsigned int seed = 42
    ) {
        std::mt19937 rng(seed);
        std::vector<std::shared_ptr<PCB>> transactions;
        transactions.reserve(num_transactions);

        // 20 peak centers distributed across the simulation timeline
        std::vector<double> peak_centers;
        double tick_interval = static_cast<double>(max_tick - min_tick) / 21.0;
        for (int i = 1; i <= 20; ++i) {
            peak_centers.push_back(min_tick + i * tick_interval);
        }

        std::uniform_real_distribution<double> uniform_dist(min_tick, max_tick);
        std::normal_distribution<double> gaussian_dist(0.0, 25.0); // sigma = 25 ticks
        std::uniform_int_distribution<size_t> peak_picker(0, peak_centers.size() - 1);
        std::uniform_real_distribution<double> type_dist(0.0, 1.0);
        std::uniform_real_distribution<double> cluster_selector(0.0, 1.0);

        // Retail accounts: 1001 - 1008
        std::vector<int> retail_accounts = {1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008};
        std::uniform_int_distribution<size_t> retail_picker(0, retail_accounts.size() - 1);

        // Corporate accounts: 2001 - 2002
        std::vector<int> corporate_accounts = {2001, 2002};
        std::uniform_int_distribution<size_t> corporate_picker(0, corporate_accounts.size() - 1);

        // ATM cash vault reserves: 5001 - 5002
        std::vector<int> atm_vaults = {5001, 5002};
        std::uniform_int_distribution<size_t> atm_picker(0, atm_vaults.size() - 1);

        // Burst distributions
        std::uniform_int_distribution<int> upi_burst_dist(1, 4);
        std::uniform_int_distribution<int> atm_burst_dist(2, 6);
        std::uniform_int_distribution<int> salary_burst_dist(10, 30);

        // Amounts
        std::uniform_real_distribution<double> upi_amt_dist(50.0, 1500.0);
        std::uniform_real_distribution<double> atm_amt_dist(100.0, 2000.0);
        std::uniform_real_distribution<double> salary_amt_dist(10000.0, 50000.0);

        for (size_t i = 0; i < num_transactions; ++i) {
            int pid = static_cast<int>(i + 1);

            // Bimodal burst clustering:
            // 75% around 20 peak centers (sigma = 25 ticks)
            // 25% uniform background
            int arrival_tick = 0;
            if (cluster_selector(rng) < 0.75) {
                size_t center_idx = peak_picker(rng);
                double center = peak_centers[center_idx];
                double sample = center + gaussian_dist(rng);
                arrival_tick = static_cast<int>(std::round(sample));
                if (arrival_tick < min_tick) arrival_tick = min_tick;
                if (arrival_tick > max_tick) arrival_tick = max_tick;
            } else {
                arrival_tick = static_cast<int>(std::round(uniform_dist(rng)));
            }

            // Transaction Types:
            // 60% UPI, 25% ATM, 15% SALARY
            double t_roll = type_dist(rng);
            TransactionType type;
            int burst = 0;
            int from_acc = 0;
            int to_acc = 0;
            double amount = 0.0;
            std::vector<int> required_locks;

            if (t_roll < 0.60) {
                // UPI: 60% traffic, 1-4 ticks burst, requires 2 retail account locks
                type = TransactionType::UPI;
                burst = upi_burst_dist(rng);
                size_t idx1 = retail_picker(rng);
                size_t idx2 = retail_picker(rng);
                while (idx2 == idx1) {
                    idx2 = retail_picker(rng);
                }
                from_acc = retail_accounts[idx1];
                to_acc = retail_accounts[idx2];
                amount = std::round(upi_amt_dist(rng) * 100.0) / 100.0;
                required_locks = {from_acc, to_acc};
            } else if (t_roll < 0.85) {
                // ATM: 25% traffic, 2-6 ticks burst, requires 1 retail + 1 ATM vault lock
                type = TransactionType::ATM;
                burst = atm_burst_dist(rng);
                from_acc = retail_accounts[retail_picker(rng)];
                to_acc = atm_vaults[atm_picker(rng)];
                amount = std::round(atm_amt_dist(rng) * 100.0) / 100.0;
                required_locks = {from_acc, to_acc};
            } else {
                // SALARY: 15% traffic, 10-30 ticks burst, corporate treasury + multiple employee locks
                type = TransactionType::SALARY;
                burst = salary_burst_dist(rng);
                from_acc = corporate_accounts[corporate_picker(rng)];
                to_acc = retail_accounts[retail_picker(rng)];
                amount = std::round(salary_amt_dist(rng) * 100.0) / 100.0;

                // Corporate treasury + 2 to 4 retail accounts
                required_locks.push_back(from_acc);
                std::uniform_int_distribution<int> num_employees(2, 4);
                int emp_count = num_employees(rng);
                std::vector<int> shuffled_retail = retail_accounts;
                std::shuffle(shuffled_retail.begin(), shuffled_retail.end(), rng);
                for (int e = 0; e < emp_count; ++e) {
                    required_locks.push_back(shuffled_retail[e]);
                }
            }

            auto pcb = std::make_shared<PCB>(pid, type, arrival_tick, burst, from_acc, to_acc, amount);
            pcb->required_locks = required_locks;
            transactions.push_back(pcb);
        }

        // Chronologically sort the output vector by arrival_time
        std::sort(transactions.begin(), transactions.end(), [](const std::shared_ptr<PCB>& a, const std::shared_ptr<PCB>& b) {
            if (a->arrival_time != b->arrival_time) {
                return a->arrival_time < b->arrival_time;
            }
            return a->pid < b->pid;
        });

        return transactions;
    }
};

#endif // BANK_OS_WORKLOAD_GENERATOR_HPP
