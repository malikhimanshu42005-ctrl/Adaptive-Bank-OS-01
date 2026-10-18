#ifndef BANK_OS_MIDDLEWARE_HPP
#define BANK_OS_MIDDLEWARE_HPP

#include <vector>
#include <deque>
#include <cmath>
#include <algorithm>
#include <mutex>
#include <string>
#include "json.hpp"
#include "pcb.hpp"

enum class ContentionLevel {
    LOW,
    MEDIUM,
    HIGH
};

inline std::string to_string(ContentionLevel lvl) {
    switch (lvl) {
        case ContentionLevel::LOW: return "LOW";
        case ContentionLevel::MEDIUM: return "MEDIUM";
        case ContentionLevel::HIGH: return "HIGH";
    }
    return "LOW";
}

struct TickTelemetry {
    int attempts = 0;
    int conflicts = 0;
    int aborts = 0;
    int commits = 0;
    int arrivals = 0;
    int batch_arrivals = 0;
    int retry_events = 0;
    int queue_length = 0;
};

struct TelemetrySnapshot {
    double conflict_rate = 0.0;
    double abort_rate = 0.0;
    int queue_length = 0;
    double retry_intensity = 0.0;
    double arrival_velocity = 0.0;
    double batch_ratio = 0.0;
    double composite_index = 0.0;
    ContentionLevel contention_level = ContentionLevel::LOW;
    bool is_adaptive = true;
    long long total_commits = 0;
    long long total_aborts = 0;
    long long total_attempts = 0;
    long long total_conflicts = 0;

    json to_json() const {
        json j = json::object();
        j["conflict_rate"] = conflict_rate;
        j["abort_rate"] = abort_rate;
        j["queue_length"] = queue_length;
        j["retry_intensity"] = retry_intensity;
        j["arrival_velocity"] = arrival_velocity;
        j["batch_ratio"] = batch_ratio;
        j["composite_index"] = composite_index;
        j["contention_level"] = to_string(contention_level);
        j["is_adaptive"] = is_adaptive;
        j["total_commits"] = total_commits;
        j["total_aborts"] = total_aborts;
        j["total_attempts"] = total_attempts;
        j["total_conflicts"] = total_conflicts;
        return j;
    }
};

class MDACCMiddleware {
private:
    bool is_adaptive_mode_ = true; // True: MD-ACC, False: Standard Baseline (2PL/OCC without backoff)
    int window_size_ = 100;        // 100-tick rolling window

    std::deque<TickTelemetry> window_;
    TickTelemetry current_tick_data_;

    // Lifetime metrics
    long long total_attempts_ = 0;
    long long total_conflicts_ = 0;
    long long total_aborts_ = 0;
    long long total_commits_ = 0;

    // Hysteresis gate to prevent oscillation
    ContentionLevel last_stable_level_ = ContentionLevel::LOW;
    int hysteresis_counter_ = 0;
    const int hysteresis_gate_ticks_ = 5;

    // Dynamic Token Bucket for rate limiting
    double token_bucket_capacity_ = 100.0;
    double token_bucket_tokens_ = 100.0;
    double token_bucket_refill_rate_ = 50.0; // tokens per tick

    mutable std::mutex mdacc_mutex_;

public:
    explicit MDACCMiddleware(bool adaptive = true, int window_size = 100)
        : is_adaptive_mode_(adaptive), window_size_(window_size) {}

    void set_adaptive_mode(bool adaptive) {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        is_adaptive_mode_ = adaptive;
    }

    bool is_adaptive_mode() const {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        return is_adaptive_mode_;
    }

    void record_attempt() {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.attempts++;
        total_attempts_++;
    }

    void record_conflict() {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.conflicts++;
        total_conflicts_++;
    }

    void record_abort() {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.aborts++;
        total_aborts_++;
    }

    void record_commit() {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.commits++;
        total_commits_++;
    }

    void record_arrival(TransactionType type) {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.arrivals++;
        if (type == TransactionType::SALARY) {
            current_tick_data_.batch_arrivals++;
        }
    }

    void record_retry() {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.retry_events++;
    }

    void update_queue_length(int ql) {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        current_tick_data_.queue_length = ql;
    }

    void advance_tick() {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        window_.push_back(current_tick_data_);
        if (static_cast<int>(window_.size()) > window_size_) {
            window_.pop_front();
        }
        current_tick_data_ = TickTelemetry();

        // Refill token bucket
        token_bucket_tokens_ = std::min(token_bucket_capacity_, token_bucket_tokens_ + token_bucket_refill_rate_);
    }

    TelemetrySnapshot get_telemetry_snapshot() const {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        return compute_snapshot_locked();
    }

    // Calculates exponential backoff based on contention and mode
    int calculate_backoff(int pid, int retry_count) {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        
        // In Standard Baseline mode: Backoff is disabled (0 ticks) to force immediate retries
        if (!is_adaptive_mode_) {
            return 0;
        }

        TelemetrySnapshot snap = compute_snapshot_locked();

        // If Contention is HIGH (or MEDIUM): Apply exponential backoff
        // Backoff = min(B0 * 2^retry_count + (pid % 3), MaxBackoff) with B0 = 2, Max = 32
        if (snap.contention_level == ContentionLevel::HIGH) {
            int b0 = 2;
            int max_backoff = 32;
            int exp_factor = 1 << std::min(retry_count, 6);
            int backoff = b0 * exp_factor + (pid % 3);
            return std::min(backoff, max_backoff);
        } else if (snap.contention_level == ContentionLevel::MEDIUM) {
            int b0 = 1;
            int max_backoff = 12;
            int exp_factor = 1 << std::min(retry_count, 4);
            int backoff = b0 * exp_factor + (pid % 2);
            return std::min(backoff, max_backoff);
        }

        // In LOW contention: Minimal backoff (1 tick if retried)
        return (retry_count > 0) ? 1 : 0;
    }

    // Dynamic Token Bucket consumption check
    bool try_consume_token(double tokens = 1.0) {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        if (token_bucket_tokens_ >= tokens) {
            token_bucket_tokens_ -= tokens;
            return true;
        }
        return false;
    }

    void set_token_bucket_params(double capacity, double refill_rate) {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        token_bucket_capacity_ = capacity;
        token_bucket_refill_rate_ = refill_rate;
        token_bucket_tokens_ = std::min(token_bucket_tokens_, capacity);
    }

    double get_token_bucket_tokens() const {
        std::lock_guard<std::mutex> lock(mdacc_mutex_);
        return token_bucket_tokens_;
    }

    // Direct helper for tests verifying formula
    static ContentionLevel evaluate_contention_rules(double cr, double ar, int ql) {
        if (cr >= 0.40 || ar >= 0.25 || ql >= 35) {
            return ContentionLevel::HIGH;
        }
        if (cr >= 0.15 || ql >= 15) {
            return ContentionLevel::MEDIUM;
        }
        return ContentionLevel::LOW;
    }

    // Continuous composite index: Ct in [0, 1]
    static double compute_composite_index(double cr, double ar, int ql) {
        double norm_ql = std::min(1.0, static_cast<double>(ql) / 50.0);
        double c_t = 0.45 * std::min(1.0, cr) + 0.35 * std::min(1.0, ar) + 0.20 * norm_ql;
        return std::clamp(c_t, 0.0, 1.0);
    }

private:
    TelemetrySnapshot compute_snapshot_locked() const {
        TelemetrySnapshot snap;
        snap.is_adaptive = is_adaptive_mode_;
        snap.total_commits = total_commits_;
        snap.total_aborts = total_aborts_;
        snap.total_attempts = total_attempts_;
        snap.total_conflicts = total_conflicts_;

        int win_attempts = 0;
        int win_conflicts = 0;
        int win_aborts = 0;
        int win_arrivals = 0;
        int win_batch = 0;
        int win_retries = 0;
        int latest_ql = current_tick_data_.queue_length;

        for (const auto& t : window_) {
            win_attempts += t.attempts;
            win_conflicts += t.conflicts;
            win_aborts += t.aborts;
            win_arrivals += t.arrivals;
            win_batch += t.batch_arrivals;
            win_retries += t.retry_events;
            latest_ql = t.queue_length;
        }

        // Include current tick
        win_attempts += current_tick_data_.attempts;
        win_conflicts += current_tick_data_.conflicts;
        win_aborts += current_tick_data_.aborts;
        win_arrivals += current_tick_data_.arrivals;
        win_batch += current_tick_data_.batch_arrivals;
        win_retries += current_tick_data_.retry_events;

        snap.conflict_rate = (win_attempts > 0) ? (static_cast<double>(win_conflicts) / win_attempts) : 0.0;
        snap.abort_rate = (win_attempts > 0) ? (static_cast<double>(win_aborts) / win_attempts) : 0.0;
        snap.queue_length = latest_ql;
        snap.retry_intensity = (win_attempts > 0) ? (static_cast<double>(win_retries) / win_attempts) : 0.0;
        snap.arrival_velocity = static_cast<double>(win_arrivals) / std::max(1, static_cast<int>(window_.size() + 1));
        snap.batch_ratio = (win_arrivals > 0) ? (static_cast<double>(win_batch) / win_arrivals) : 0.0;

        snap.composite_index = compute_composite_index(snap.conflict_rate, snap.abort_rate, snap.queue_length);
        
        // Raw level from threshold rules
        ContentionLevel raw_level = evaluate_contention_rules(snap.conflict_rate, snap.abort_rate, snap.queue_length);

        // Hysteresis gate to prevent high-frequency flapping
        if (raw_level != last_stable_level_) {
            const_cast<MDACCMiddleware*>(this)->hysteresis_counter_++;
            if (hysteresis_counter_ >= hysteresis_gate_ticks_) {
                const_cast<MDACCMiddleware*>(this)->last_stable_level_ = raw_level;
                const_cast<MDACCMiddleware*>(this)->hysteresis_counter_ = 0;
            }
        } else {
            const_cast<MDACCMiddleware*>(this)->hysteresis_counter_ = 0;
        }

        snap.contention_level = last_stable_level_;
        return snap;
    }
};

#endif // BANK_OS_MIDDLEWARE_HPP
