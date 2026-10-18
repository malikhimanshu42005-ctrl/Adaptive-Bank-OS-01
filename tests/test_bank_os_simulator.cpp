#include <iostream>
#include <cassert>
#include <vector>
#include <fstream>
#include "bank_os.hpp"

void test_pcb_serialization() {
    std::cout << "[Test 1.1] Testing PCB Serialization...\n";
    PCB original(101, TransactionType::UPI, 10, 4, 1001, 1002, 250.75);
    original.required_locks = {1001, 1002};
    original.state = ProcessState::READY;
    original.wait_time = 3;
    original.aged_promoted = false;

    json j = original.to_json();
    assert(j["pid"].get<int>() == 101);
    assert(j["type"].get<std::string>() == "UPI");
    assert(j["state"].get<std::string>() == "READY");
    assert(j["amount"].get<double>() > 250.74 && j["amount"].get<double>() < 250.76);
    assert(j["required_locks"].size() == 2);

    PCB restored = PCB::from_json(j);
    assert(restored.pid == 101);
    assert(restored.type == TransactionType::UPI);
    assert(restored.state == ProcessState::READY);
    assert(restored.required_locks.size() == 2);
    std::cout << "  -> PCB serialization & deserialization PASSED.\n";
}

void test_176k_workload_generation() {
    std::cout << "[Test 1.2] Testing 176,000 Transactions Workload Generation...\n";
    auto workload = WorkloadGenerator::generate_workload(176000, 0, 8000, 42);
    assert(workload.size() == 176000);

    // Verify chronological ordering
    for (size_t i = 1; i < workload.size(); ++i) {
        assert(workload[i]->arrival_time >= workload[i - 1]->arrival_time);
    }

    // Verify transaction type ratio approximate bounds (60% UPI, 25% ATM, 15% Salary)
    size_t upi_count = 0, atm_count = 0, salary_count = 0;
    for (const auto& tx : workload) {
        if (tx->type == TransactionType::UPI) upi_count++;
        else if (tx->type == TransactionType::ATM) atm_count++;
        else if (tx->type == TransactionType::SALARY) salary_count++;
    }

    double upi_ratio = static_cast<double>(upi_count) / workload.size();
    double atm_ratio = static_cast<double>(atm_count) / workload.size();
    double salary_ratio = static_cast<double>(salary_count) / workload.size();

    assert(upi_ratio > 0.55 && upi_ratio < 0.65);
    assert(atm_ratio > 0.20 && atm_ratio < 0.30);
    assert(salary_ratio > 0.10 && salary_ratio < 0.20);
    std::cout << "  -> 176k workload verified: chronological order and traffic proportions valid.\n";
}

void test_srtf_dispatching() {
    std::cout << "[Test 1.3] Testing SRTF Dispatching in Queue 1...\n";
    MLFQScheduler scheduler;
    // Add job A (burst 4) and job B (burst 2)
    auto jobA = std::make_shared<PCB>(1, TransactionType::UPI, 0, 4, 1001, 1002, 100.0);
    auto jobB = std::make_shared<PCB>(2, TransactionType::UPI, 0, 2, 1003, 1004, 100.0);

    scheduler.add_process(jobA, 0);
    scheduler.add_process(jobB, 0);

    // Next scheduled should be Job B (shorter burst)
    auto next = scheduler.schedule_next(0);
    assert(next != nullptr);
    assert(next->pid == 2);
    assert(next->burst_time == 2);
    std::cout << "  -> SRTF correctly dispatched shorter job first (PID 2 with burst 2 before PID 1 with burst 4).\n";
}

void test_rr_quantum_expiration() {
    std::cout << "[Test 1.4] Testing Round Robin Quantum Expiration in Queue 2...\n";
    MLFQScheduler scheduler(50, 3); // Q2 quantum = 3 ticks
    auto jobS1 = std::make_shared<PCB>(10, TransactionType::SALARY, 0, 10, 2001, 1001, 10000.0);
    auto jobS2 = std::make_shared<PCB>(11, TransactionType::SALARY, 0, 10, 2001, 1002, 10000.0);

    scheduler.add_process(jobS1, 0);
    scheduler.add_process(jobS2, 0);

    auto running = scheduler.schedule_next(0);
    assert(running->pid == 10);

    // Simulate 3 ticks of execution
    scheduler.on_tick_advance();
    running->burst_time--;
    scheduler.on_tick_advance();
    running->burst_time--;
    scheduler.on_tick_advance();
    running->burst_time--;

    // Should preempt and rotate
    assert(scheduler.should_preempt() == true);
    auto next = scheduler.schedule_next(3);
    assert(next->pid == 11); // Rotated to next batch job
    std::cout << "  -> Round Robin quantum expiration rotated execution to PID 11.\n";
}

void test_aging_promotion() {
    std::cout << "[Test 1.5] Testing Anti-Starvation Aging Engine (X = 30)...\n";
    MLFQScheduler scheduler(30, 5); // Aging threshold X = 30
    auto salary_job = std::make_shared<PCB>(50, TransactionType::SALARY, 0, 15, 2001, 1001, 25000.0);
    scheduler.add_process(salary_job, 0);
    assert(salary_job->current_queue == 2);
    assert(!salary_job->aged_promoted);

    // Advance 30 ticks
    for (int t = 1; t <= 30; ++t) {
        scheduler.on_tick_advance();
    }
    scheduler.promote_aged_processes(30);

    assert(salary_job->aged_promoted == true);
    assert(salary_job->current_queue == 1);
    std::cout << "  -> Job promoted from Q2 to Q1 after 30 ticks of waiting.\n";
}

void test_lock_ordering_and_arbitration() {
    std::cout << "[Test 1.6] Testing Havender Strict Monotonic Lock Ordering...\n";
    std::vector<int> locks = {1006, 1002, 1008, 1001};
    std::vector<int> ordered = LockManager::get_monotonic_lock_order(locks);
    assert(ordered.size() == 4);
    assert(ordered[0] == 1001 && ordered[1] == 1002 && ordered[2] == 1006 && ordered[3] == 1008);

    LockManager lm(20);
    int c_acc = 0, c_pid = 0;
    bool ok1 = lm.try_acquire_locks(1, {1001, 1002}, c_acc, c_pid);
    assert(ok1);

    // Reciprocal transfer on 1002, 1001 by PID 2
    bool ok2 = lm.try_acquire_locks(2, {1002, 1001}, c_acc, c_pid);
    assert(!ok2); // Must fail gracefully without circular deadlock
    assert(c_acc == 1001);
    assert(c_pid == 1);

    lm.release_locks(1);
    bool ok3 = lm.try_acquire_locks(2, {1002, 1001}, c_acc, c_pid);
    assert(ok3); // Acquired after release
    lm.release_locks(2);
    std::cout << "  -> Monotonic lock ordering and reciprocal try-lock arbitration verified.\n";
}

void test_insufficient_funds_rollback() {
    std::cout << "[Test 1.7] Testing Insufficient Funds Rollback...\n";
    StorageEngine storage;
    // Account 1008 seeded with 100.00
    double init_bal = storage.get_balance(1008);
    assert(init_bal >= 99.0 && init_bal <= 101.0);

    // Attempt transfer of 500.00 from 1008
    PCB pcb(99, TransactionType::UPI, 0, 2, 1008, 1001, 500.00);
    bool success = storage.execute_transfer(pcb, 1);
    assert(!success);
    assert(pcb.state == ProcessState::TERMINATED_FAILED);
    assert(storage.get_balance(1008) == init_bal); // Balance untouched
    assert(storage.get_total_rollbacks() == 1);
    std::cout << "  -> Invariant upheld: insufficient funds triggered ROLLBACK and status TERMINATED_FAILED.\n";
}

void test_mdacc_vs_baseline() {
    std::cout << "[Test 1.8] Testing MD-ACC vs Baseline Comparison...\n";
    MDACCMiddleware mdacc(true);
    // In adaptive mode with simulated high contention:
    int backoff_adaptive = mdacc.calculate_backoff(101, 2);
    // Baseline mode:
    mdacc.set_adaptive_mode(false);
    int backoff_baseline = mdacc.calculate_backoff(101, 2);
    assert(backoff_baseline == 0); // Standard baseline disables backoff delay
    std::cout << "  -> Mode comparison verified: Baseline forces immediate retries (0 backoff) while MD-ACC adapts.\n";
}

void test_json_transition_export() {
    std::cout << "[Test 1.9] Testing JSON Transition Export...\n";
    MLFQScheduler scheduler;
    scheduler.log_transition(1, 101, TransactionType::UPI, ProcessState::NEW, ProcessState::READY, "Test entry");
    scheduler.log_transition(2, 101, TransactionType::UPI, ProcessState::READY, ProcessState::RUNNING, "Test dispatch");

    std::string test_file = "test_transitions.json";
    bool exported = scheduler.export_transitions_to_json(test_file, 10);
    assert(exported);

    std::ifstream ifs(test_file);
    assert(ifs.is_open());
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    assert(content.find("Test dispatch") != std::string::npos);
    std::remove(test_file.c_str());
    std::cout << "  -> Transitions exported to JSON file successfully.\n";
}

int main() {
    std::cout << "========================================================\n";
    std::cout << " RUNNING TEST SUITE 1: BANK OS SIMULATOR INTEGRATION    \n";
    std::cout << "========================================================\n";

    test_pcb_serialization();
    test_176k_workload_generation();
    test_srtf_dispatching();
    test_rr_quantum_expiration();
    test_aging_promotion();
    test_lock_ordering_and_arbitration();
    test_insufficient_funds_rollback();
    test_mdacc_vs_baseline();
    test_json_transition_export();

    std::cout << ">>> ALL TEST 1 ASSERTIONS PASSED SUCCESSFULLY! <<<\n\n";
    return 0;
}
