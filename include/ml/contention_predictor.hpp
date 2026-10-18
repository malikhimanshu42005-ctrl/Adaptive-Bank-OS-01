#ifndef BANK_OS_CONTENTION_PREDICTOR_HPP
#define BANK_OS_CONTENTION_PREDICTOR_HPP

#include <vector>
#include <array>
#include <cmath>
#include <chrono>
#include <string>
#include <algorithm>
#include "model_weights.hpp"

enum class TrafficRegime {
    NORMAL,
    UPI_EVENING_PEAK,
    SALARY_CLEARING_BURST
};

inline std::string to_string(TrafficRegime regime) {
    switch (regime) {
        case TrafficRegime::NORMAL: return "NORMAL";
        case TrafficRegime::UPI_EVENING_PEAK: return "UPI_EVENING_PEAK";
        case TrafficRegime::SALARY_CLEARING_BURST: return "SALARY_CLEARING_BURST";
    }
    return "NORMAL";
}

struct FeatureVector {
    float conflict_rate = 0.0f;
    float abort_rate = 0.0f;
    float queue_depth = 0.0f;
    float retry_intensity = 0.0f;
    float arrival_velocity = 0.0f;
    float batch_ratio = 0.0f;

    float get(int idx) const {
        switch (idx) {
            case 0: return conflict_rate;
            case 1: return abort_rate;
            case 2: return queue_depth;
            case 3: return retry_intensity;
            case 4: return arrival_velocity;
            case 5: return batch_ratio;
            default: return 0.0f;
        }
    }
};

class ContentionPredictor {
private:
    std::array<FeatureVector, ml_weights::WINDOW_SIZE> window_{};
    size_t head_ = 0;
    size_t count_ = 0;
    double last_latency_ns_ = 0.0;

    static float relu(float x) {
        return (x > 0.0f) ? x : 0.0f;
    }

    static float sigmoid(float x) {
        return 1.0f / (1.0f + std::exp(-x));
    }

public:
    ContentionPredictor() = default;

    void ingest_features(float cr, float ar, float qd, float ri, float av, float br) {
        FeatureVector fv{cr, ar, qd, ri, av, br};
        window_[head_] = fv;
        head_ = (head_ + 1) % ml_weights::WINDOW_SIZE;
        if (count_ < ml_weights::WINDOW_SIZE) {
            count_++;
        }
    }

    void ingest(const FeatureVector& fv) {
        window_[head_] = fv;
        head_ = (head_ + 1) % ml_weights::WINDOW_SIZE;
        if (count_ < ml_weights::WINDOW_SIZE) {
            count_++;
        }
    }

    // Regime classification based on switch stream feature signatures
    TrafficRegime classify_regime() const {
        if (count_ == 0) return TrafficRegime::NORMAL;

        // Inspect recent window average
        float avg_cr = 0.0f, avg_ar = 0.0f, avg_qd = 0.0f, avg_av = 0.0f, avg_br = 0.0f;
        for (size_t i = 0; i < count_; ++i) {
            avg_cr += window_[i].conflict_rate;
            avg_ar += window_[i].abort_rate;
            avg_qd += window_[i].queue_depth;
            avg_av += window_[i].arrival_velocity;
            avg_br += window_[i].batch_ratio;
        }
        avg_cr /= count_;
        avg_ar /= count_;
        avg_qd /= count_;
        avg_av /= count_;
        avg_br /= count_;

        if (avg_br >= 0.40 || (avg_br >= 0.30 && avg_qd >= 0.50)) {
            return TrafficRegime::SALARY_CLEARING_BURST;
        }
        if (avg_av >= 0.50 && avg_cr >= 0.25) {
            return TrafficRegime::UPI_EVENING_PEAK;
        }
        return TrafficRegime::NORMAL;
    }

    // Pure C++ forward pass with sub-microsecond latency (<2µs)
    // Forecasts contention Ct+30ms 30ms ahead of reactive metrics
    float predict_contention() {
        auto t_start = std::chrono::high_resolution_clock::now();

        // 1. Gather chronological window [W=8, C=6]
        float input_buf[ml_weights::WINDOW_SIZE][ml_weights::NUM_FEATURES];
        for (int t = 0; t < ml_weights::WINDOW_SIZE; ++t) {
            size_t idx = (head_ + t) % ml_weights::WINDOW_SIZE;
            for (int f = 0; f < ml_weights::NUM_FEATURES; ++f) {
                input_buf[t][f] = window_[idx].get(f);
            }
        }

        // 2. Conv1D Layer 1: [8 out_channels, 6 in_channels, kernel 3]
        // Output length = 8 - 3 + 1 = 6 steps
        constexpr int CONV1_LEN = ml_weights::WINDOW_SIZE - ml_weights::KERNEL_SIZE + 1; // 6
        float conv1_out[CONV1_LEN][ml_weights::CONV1_OUT];

        for (int t = 0; t < CONV1_LEN; ++t) {
            for (int o = 0; o < ml_weights::CONV1_OUT; ++o) {
                float sum = ml_weights::CONV1_BIAS[o];
                for (int c = 0; c < ml_weights::NUM_FEATURES; ++c) {
                    for (int k = 0; k < ml_weights::KERNEL_SIZE; ++k) {
                        sum += input_buf[t + k][c] * ml_weights::CONV1_WEIGHTS[o][c][k];
                    }
                }
                conv1_out[t][o] = relu(sum);
            }
        }

        // 3. Conv1D Layer 2: [4 out_channels, 8 in_channels, kernel 3]
        // Output length = 6 - 3 + 1 = 4 steps
        constexpr int CONV2_LEN = CONV1_LEN - ml_weights::KERNEL_SIZE + 1; // 4
        float conv2_out[CONV2_LEN][ml_weights::CONV2_OUT];

        for (int t = 0; t < CONV2_LEN; ++t) {
            for (int o = 0; o < ml_weights::CONV2_OUT; ++o) {
                float sum = ml_weights::CONV2_BIAS[o];
                for (int c = 0; c < ml_weights::CONV1_OUT; ++c) {
                    for (int k = 0; k < ml_weights::KERNEL_SIZE; ++k) {
                        sum += conv1_out[t + k][c] * ml_weights::CONV2_WEIGHTS[o][c][k];
                    }
                }
                conv2_out[t][o] = relu(sum);
            }
        }

        // 4. Global Average Pooling over temporal dimension [4 steps -> 1 step]
        float pooled[ml_weights::CONV2_OUT]{};
        for (int o = 0; o < ml_weights::CONV2_OUT; ++o) {
            float sum = 0.0f;
            for (int t = 0; t < CONV2_LEN; ++t) {
                sum += conv2_out[t][o];
            }
            pooled[o] = sum / CONV2_LEN;
        }

        // 5. Output Dense Linear Layer: 4 -> 1 + Sigmoid
        float dense_sum = ml_weights::DENSE_BIAS;
        for (int i = 0; i < ml_weights::CONV2_OUT; ++i) {
            dense_sum += pooled[i] * ml_weights::DENSE_WEIGHTS[i];
        }

        // Also factor in recent direct indicators to guarantee robust bounding
        float raw_cr = window_[(head_ + ml_weights::WINDOW_SIZE - 1) % ml_weights::WINDOW_SIZE].conflict_rate;
        float raw_ar = window_[(head_ + ml_weights::WINDOW_SIZE - 1) % ml_weights::WINDOW_SIZE].abort_rate;
        float blended = 0.70f * sigmoid(dense_sum) + 0.20f * raw_cr + 0.10f * raw_ar;

        float prediction = std::clamp(blended, 0.0f, 1.0f);

        auto t_end = std::chrono::high_resolution_clock::now();
        last_latency_ns_ = std::chrono::duration<double, std::nano>(t_end - t_start).count();

        return prediction;
    }

    double get_last_latency_ns() const {
        return last_latency_ns_;
    }

    size_t window_count() const {
        return count_;
    }
};

#endif // BANK_OS_CONTENTION_PREDICTOR_HPP
