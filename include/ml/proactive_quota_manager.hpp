#ifndef BANK_OS_PROACTIVE_QUOTA_MANAGER_HPP
#define BANK_OS_PROACTIVE_QUOTA_MANAGER_HPP

#include <algorithm>
#include <iostream>
#include "contention_predictor.hpp"

struct QuotaAllocation {
    double q1_priority_quota = 0.60;  // 60% default quota for high priority Q1
    int q2_batch_quantum = 5;         // 5 ticks (200ms) default quantum for batch Q2
    double token_bucket_refill = 50.0;// tokens/sec
    bool is_compressed = false;
};

class ProactiveQuotaManager {
private:
    QuotaAllocation current_quota_;
    double contention_threshold_ = 0.50; // Threshold to trigger proactive mitigation

public:
    ProactiveQuotaManager() = default;

    // Proactively compute and adjust quota allocation based on forecasted contention and traffic regime
    QuotaAllocation evaluate_proactive_quota(float predicted_contention, TrafficRegime regime) {
        QuotaAllocation alloc;

        if (predicted_contention >= contention_threshold_ || regime != TrafficRegime::NORMAL) {
            // High contention or burst regime: Pre-emptively compress batch quantum
            // and raise Q1 priority quota to 85% before burst lock convoying occurs
            alloc.is_compressed = true;
            alloc.q1_priority_quota = 0.85; // Raised to 85%
            
            // Compress batch quantum from 5 ticks (200ms) down to 1 tick (40ms)
            alloc.q2_batch_quantum = 1; 

            // Throttle token bucket refill rate
            alloc.token_bucket_refill = 20.0;
        } else {
            // Normal regime: standard operational parameters
            alloc.is_compressed = false;
            alloc.q1_priority_quota = 0.60; // 60%
            alloc.q2_batch_quantum = 5;    // 5 ticks (200ms)
            alloc.token_bucket_refill = 50.0;
        }

        current_quota_ = alloc;
        return alloc;
    }

    QuotaAllocation get_current_quota() const {
        return current_quota_;
    }

    void set_contention_threshold(double th) {
        contention_threshold_ = th;
    }

    double get_contention_threshold() const {
        return contention_threshold_;
    }
};

#endif // BANK_OS_PROACTIVE_QUOTA_MANAGER_HPP
