# Adaptive Bank OS

> **An OS-kernel-inspired banking transaction simulator that processes 176,000 concurrent transactions using adaptive scheduling, ML-driven contention prediction, and ACID-compliant storage — all visualized through a real-time web dashboard.**

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Core Modules](#core-modules)
  - [Process Control Block (PCB)](#process-control-block-pcb)
  - [MLFQ Scheduler](#mlfq-scheduler)
  - [Lock Manager](#lock-manager)
  - [MD-ACC Middleware](#md-acc-middleware)
  - [ML Contention Predictor](#ml-contention-predictor)
  - [Storage Engine](#storage-engine)
  - [Workload Generator](#workload-generator)
- [Web Dashboard](#web-dashboard)
- [REST API](#rest-api)
- [Database Schema](#database-schema)
- [Building & Running](#building--running)
- [Testing](#testing)
- [ML Model Training](#ml-model-training)
- [License](#license)

---

## Overview

Adaptive Bank OS models a high-throughput banking switch as an operating system kernel. Each financial transaction (UPI, ATM, Salary) is represented as a **process** with a full lifecycle — creation, scheduling, lock acquisition, CPU execution, ACID commit/rollback, and termination — mirroring the `NEW → READY → RUNNING → WAITING → TERMINATED` state machine of a real OS.

The system generates **176,000 transactions** with realistic burst patterns, schedules them through a **Multi-Level Feedback Queue (MLFQ)**, manages concurrent account access via **deadlock-free lock ordering**, and dynamically adapts to contention using an embedded **Temporal Convolutional Network (TCN)** for real-time contention prediction.

---

## Key Features

| Feature | Description |
|---|---|
| **176K Transaction Workload** | Bimodal burst clustering with 20 Gaussian peak centers across 8,000 simulation ticks |
| **MLFQ Scheduling** | Queue 1 (SRTF for UPI/ATM) + Queue 2 (Round Robin for Salary batches) |
| **Anti-Starvation Aging** | Salary transactions promoted from Q2 → Q1 after configurable threshold ticks |
| **Deadlock-Free Locking** | Havender's Principle — strict monotonic lock ordering with bounded wait timeout (Tw) |
| **MD-ACC Middleware** | Multi-Dimensional Adaptive Concurrency Control with hysteresis-gated contention levels |
| **ML Contention Prediction** | Lightweight TCN (1D Conv) predicting contention 30ms ahead from 6-feature telemetry |
| **Proactive Quota Management** | Dynamic Q2 quantum compression based on predicted contention regime |
| **ACID Storage Engine** | Write-Ahead Logging (WAL) with debit-credit atomicity and insufficient-funds rollback |
| **Exponential Backoff** | Adaptive retry with jitter: `B₀ × 2^retry + (PID % 3)`, capped at 32 ticks |
| **Comparison Mode** | Toggle between `ADAPTIVE_MDACC` and `STANDARD_BASELINE` to measure improvement |
| **Real-Time Dashboard** | Live web UI with process state visualization, telemetry charts, and queue inspection |
| **REST API** | Full HTTP API for status, state, queues, transitions, speed control, and data export |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Web Dashboard (HTML/CSS/JS)             │
│            Real-time polling · Charts · Queue Viz           │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP REST API
┌──────────────────────────▼──────────────────────────────────┐
│                    HTTP Server (httplib)                     │
│           /api/status · /api/state · /api/queues            │
│        /api/transitions · /api/control/* · /api/export      │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                   BankOSSimulator (Core)                     │
│                                                             │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │  Workload    │  │    MLFQ      │  │   Lock Manager    │  │
│  │  Generator   │──│  Scheduler   │──│  (Havender's)     │  │
│  │  (176K txns) │  │  Q1:SRTF     │  │  Bounded Tw       │  │
│  └─────────────┘  │  Q2:RR       │  └───────────────────┘  │
│                    │  Aging Engine │                         │
│                    └──────────────┘                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │            MD-ACC Middleware                          │   │
│  │  Telemetry Window · Contention Rules · Token Bucket  │   │
│  │  Hysteresis Gate · Exponential Backoff Calculator     │   │
│  └────────────────────────┬────────────────────────────┘   │
│                           │                                 │
│  ┌────────────────────────▼────────────────────────────┐   │
│  │          ML Subsystem                                │   │
│  │  ContentionPredictor (TCN)  ·  QuotaManager          │   │
│  │  6-feature input · Regime Classification             │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │          Storage Engine (ACID)                        │   │
│  │  WAL · Debit/Credit Atomicity · Balance Tracking     │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
Adaptive-Bank-OS-01/
├── src/
│   └── main.cpp                    # Entry point: HTTP server + simulation loop
├── include/
│   ├── bank_os.hpp                 # Core simulator orchestrator
│   ├── pcb.hpp                     # Process Control Block definition
│   ├── mlfq_scheduler.hpp          # Multi-Level Feedback Queue scheduler
│   ├── lock_manager.hpp            # Deadlock-free lock manager
│   ├── middleware.hpp              # MD-ACC adaptive concurrency middleware
│   ├── workload_generator.hpp      # 176K transaction workload generator
│   ├── mysql_storage.hpp           # ACID storage engine with WAL
│   ├── httplib.h                   # Embedded HTTP server library
│   ├── json.hpp                    # Lightweight JSON library
│   └── ml/
│       ├── contention_predictor.hpp  # TCN inference engine (C++)
│       ├── model_weights.hpp         # Auto-generated TCN weights
│       └── proactive_quota_manager.hpp  # Dynamic quota allocation
├── ml/
│   └── train_model.py              # Python training script → exports C++ weights
├── web/
│   ├── index.html                  # Dashboard HTML
│   ├── styles.css                  # Dashboard styles
│   └── app.js                      # Dashboard JavaScript (polling + charts)
├── tests/
│   └── test_bank_os_simulator.cpp  # Comprehensive unit & integration tests
├── schema.sql                      # MySQL database schema (Accounts, Transactions, WAL)
└── README.md
```

---

## Core Modules

### Process Control Block (PCB)

Each transaction is encapsulated in a `PCB` struct ([`include/pcb.hpp`](include/pcb.hpp)) tracking:

- **Identity**: PID, transaction type (`UPI` / `ATM` / `SALARY`)
- **Scheduling**: arrival time, burst time (remaining CPU cycles), current queue, aged-promotion flag
- **Concurrency**: required account locks, retry count, backoff delay, abort count
- **Lifecycle**: process state (`NEW` → `READY` → `RUNNING` → `WAITING` → `TERMINATED` / `TERMINATED_FAILED`), failure reason
- **Financials**: source account, destination account, transfer amount

### MLFQ Scheduler

The scheduler ([`include/mlfq_scheduler.hpp`](include/mlfq_scheduler.hpp)) implements a two-level MLFQ:

- **Queue 1 (High Priority)**: Shortest Remaining Time First (SRTF) for interactive UPI/ATM transactions. Preempts Q2 jobs and shorter-burst Q1 jobs.
- **Queue 2 (Low Priority)**: Round Robin with configurable quantum for batch Salary transactions. Quantum is dynamically compressed by the ML quota manager under high contention.
- **Anti-Starvation Aging**: Salary transactions waiting ≥ `X` ticks in Q2 are promoted to Q1 to prevent indefinite starvation.

### Lock Manager

The lock manager ([`include/lock_manager.hpp`](include/lock_manager.hpp)) provides:

- **Deadlock prevention** via Havender's Principle — locks are always acquired in strictly ascending account-ID order
- **Atomic try-lock**: All-or-nothing acquisition; partial locks are rolled back on conflict
- **Bounded wait timeout (Tw)**: Processes exceeding `Tw` ticks in WAITING state are aborted or retried with exponential backoff

### MD-ACC Middleware

The Multi-Dimensional Adaptive Concurrency Control middleware ([`include/middleware.hpp`](include/middleware.hpp)) provides:

- **100-tick rolling telemetry window** tracking conflict rate, abort rate, queue depth, retry intensity, arrival velocity, and batch ratio
- **Composite Contention Index**: `Ct = 0.45 × CR + 0.35 × AR + 0.20 × QD_norm`, clamped to [0, 1]
- **Threshold-based contention levels**: `HIGH` (CR ≥ 0.40 or AR ≥ 0.25 or QL ≥ 35), `MEDIUM`, `LOW`
- **Hysteresis gate** (5-tick debounce) to prevent high-frequency level oscillation
- **Dynamic token bucket** for rate limiting
- **Adaptive exponential backoff**: Backoff duration scales with contention level; disabled in baseline mode to demonstrate abort storms

### ML Contention Predictor

A lightweight Temporal Convolutional Network ([`include/ml/contention_predictor.hpp`](include/ml/contention_predictor.hpp)):

- **Architecture**: 2-layer 1D ConvNet (6→8 channels, kernel 3, dilation 1) → (8→4 channels, kernel 3, dilation 2) → Dense(4→1) → Sigmoid
- **Input**: Sliding window of 8 timesteps × 6 telemetry features
- **Output**: Predicted contention index (0.0–1.0), classified into traffic regimes (`QUIET`, `MODERATE`, `STORM`, `SURGE`)
- **Training**: Synthetic banking stream data generated by [`ml/train_model.py`](ml/train_model.py); weights exported as static C++ arrays in [`model_weights.hpp`](include/ml/model_weights.hpp)

### Storage Engine

The ACID-compliant storage engine ([`include/mysql_storage.hpp`](include/mysql_storage.hpp)) simulates:

- **Write-Ahead Logging (WAL)**: PREPARE → DEBIT → CREDIT → COMMIT sequence with monotonic sequence numbers
- **Atomicity**: Failed debits/credits trigger full rollback with balance restoration
- **Insufficient funds detection**: Transactions against low-balance accounts are rolled back with descriptive failure reasons
- **Balance tracking**: In-memory balance map seeded from initial account configuration

### Workload Generator

The workload generator ([`include/workload_generator.hpp`](include/workload_generator.hpp)) creates realistic traffic:

- **176,000 transactions** with deterministic seeding (seed = 42) for reproducibility
- **Type distribution**: 60% UPI (1–4 tick burst), 25% ATM (2–6 tick burst), 15% Salary (10–30 tick burst)
- **Burst clustering**: 75% of arrivals cluster around 20 Gaussian peak centers (σ = 25 ticks); 25% uniform background
- **12 seeded accounts**: 8 retail (including 2 low-balance), 2 corporate treasury, 2 ATM vaults

---

## Web Dashboard

The real-time web dashboard (`web/`) provides:

- **Live simulation metrics** — tick counter, TPS, total commits/rollbacks, active locks
- **Process state visualization** — currently running process, queue depths, waiting pool
- **Telemetry charts** — conflict rate, abort rate, queue length, ML-predicted contention
- **Queue inspection** — Q1 (SRTF), Q2 (Round Robin), and waiting pool PIDs
- **Control panel** — pause/resume, speed adjustment, comparison mode toggle
- **State transition log** — recent process state changes with reasons
- **Data export** — download transition history as JSON

---

## REST API

| Endpoint | Method | Description |
|---|---|---|
| `/api/status` | `GET` | Tick count, queue sizes, TPS, commits, rollbacks, comparison mode |
| `/api/state` | `GET` | Full simulation state: running process, queues, telemetry, ML forecast, balances |
| `/api/queues` | `GET` | Detailed queue contents (Q1, Q2, waiting pool) |
| `/api/transitions?limit=N` | `GET` | Recent state transition records (default: 100) |
| `/api/comparison-mode` | `POST` | Toggle between `ADAPTIVE_MDACC` and `STANDARD_BASELINE` |
| `/api/control/pause` | `GET` | Pause simulation |
| `/api/control/resume` | `GET` | Resume simulation |
| `/api/control/speed?delay=N` | `GET` | Set tick delay in microseconds (lower = faster) |
| `/api/export` | `GET` | Export transition history as downloadable JSON file |

---

## Database Schema

The MySQL schema ([`schema.sql`](schema.sql)) defines three tables:

- **`Accounts`** — Account ID, type (RETAIL/CORPORATE/ATM_VAULT), balance
- **`Transactions`** — PID, type, from/to accounts, amount, status, tick
- **`WriteAheadLog`** — WAL records with sequence numbers, operation type, old/new balances

12 accounts are seeded: 8 retail (2 intentionally low-balance for rollback testing), 2 corporate treasury ($5M each), and 2 ATM vaults ($1M each).

---

## Building & Running

### Prerequisites

- C++17 compatible compiler (GCC 9+, Clang 10+, or MSVC 2019+)
- pthread support
- Python 3.6+ (for ML weight generation, optional)

### Compile

```bash
g++ -std=c++17 -O2 -pthread \
  -I include \
  src/main.cpp \
  -o bank_os_server
```

### Run

```bash
# Default: 176,000 transactions on port 8080
./bank_os_server

# Custom workload and port
./bank_os_server --workload 50000 --port 9090
```

Then open **http://localhost:8080** in your browser to view the live dashboard.

### Command-Line Options

| Flag | Default | Description |
|---|---|---|
| `--port` | `8080` | HTTP server port |
| `--workload` | `176000` | Number of transactions to generate |

---

## Testing

Compile and run the test suite:

```bash
g++ -std=c++17 -O2 -pthread \
  -I include \
  tests/test_bank_os_simulator.cpp \
  -o test_bank_os

./test_bank_os
```

The test suite covers:

- PCB serialization/deserialization
- 176K workload generation (size, ordering, type ratios)
- MLFQ scheduling (SRTF dispatching, Q2 Round Robin, aging promotion)
- Lock manager (monotonic ordering, conflict detection, timeout, release)
- MD-ACC middleware (contention thresholds, composite index, exponential backoff)
- Storage engine (WAL atomicity, insufficient funds rollback, balance integrity)
- Full simulation integration (1,000-tick end-to-end run)

---

## ML Model Training

To regenerate the TCN weights:

```bash
cd ml
python3 train_model.py
```

This generates synthetic banking stream data across three traffic regimes (UPI evening peak, salary clearing burst, quiet traffic), trains calibrated TCN weights, and exports them to `include/ml/model_weights.hpp` as static C++ float arrays — no runtime ML dependencies required.

---

## License

This project is provided for educational and research purposes.
