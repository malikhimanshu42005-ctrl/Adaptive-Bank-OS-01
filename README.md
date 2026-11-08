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
  - [Global Navigation & Keyboard Shortcuts](#global-navigation--keyboard-shortcuts)
  - [Page 1: Architecture & Execution Pipeline](#page-1-architecture--execution-pipeline)
  - [Page 2: Scheduled DB ➔ Bank DB Ledger](#page-2-scheduled-db--bank-db-ledger)
  - [Page 3: System Oscilloscope & Live Benchmarks](#page-3-system-oscilloscope--live-benchmarks)
  - [Page 4: DBMS Architecture (2PL & ACID)](#page-4-dbms-architecture-2pl--acid)
  - [Page 5: OS Kernel Architecture (MLFQ & PCB)](#page-5-os-kernel-architecture-mlfq--pcb)
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
| **MLFQ Scheduling** | Queue 1 (SRTF for UPI/ATM) + Queue 2 (Round Robin for Salary batches) with dynamic preemption |
| **Anti-Starvation Aging** | Starvation-free priority boosting: Salary transactions waiting ≥ `X` ticks in Q2 are promoted to Q1 |
| **Deadlock-Free Locking** | Strict monotonic lock ordering via Havender's Principle with bounded wait timeout ($T_w$) |
| **Havender DAG Visualizer** | Interactive visual directed acyclic graph demonstrating zero-cycle lock acquisition ($A_1 < A_2$) |
| **MD-ACC Middleware** | Multi-Dimensional Adaptive Concurrency Control with hysteresis-gated contention levels |
| **ML Contention Prediction** | Lightweight TCN (1D Conv) predicting contention 30ms ahead from 6-feature telemetry window |
| **Proactive Quota Management** | Dynamic Q2 quantum compression based on predicted traffic regime (`QUIET`, `MODERATE`, `STORM`, `SURGE`) |
| **ACID Storage Engine** | Write-Ahead Logging (WAL) with debit-credit atomicity, balance invariants, and insufficient-funds rollback |
| **Exponential Backoff** | Adaptive retry with jitter: $B_0 \times 2^{\text{retry}} + (\text{PID} \bmod 3)$, capped at 32 ticks |
| **Comparison Mode** | Real-time toggle between `ADAPTIVE_MDACC` and `STANDARD_BASELINE` to observe abort storms |
| **Multi-Page Mission Control** | 5 dedicated tabbed consoles built with geometric `Plus Jakarta Sans` and tabular `JetBrains Mono` |
| **Milestone Flow Tracer** | Real-time visual tracking of transactions moving through Ingest → MLFQ → 2PL → ACID → Settlement |
| **Hardware Oscilloscope** | Tri-channel signal monitor (`CH1 TPS`, `CH2 Contention`, `CH3 Abort Rate`) with trace freeze and live HUD |
| **Live Benchmark Suite** | 5 executable stress scenarios: `Flash Sale`, `Salary Burst`, `Hotspot Contention`, `Insufficient Funds`, `Baseline Shootout` |
| **CPU Execution Gantt Strip** | 16-slice sliding window visualizer of CPU time slices color-coded by transaction type (`UPI`, `ATM`, `SALARY`) |
| **PCB Deep Inspector** | Modal inspector displaying process registers, scheduling metrics, lock sets, and state transition logs |
| **Official Bank DB Log** | Verifiable transmission ledger with full-text search, status filters, and dual export (`JSON` + `CSV`) |
| **Keyboard Shortcuts** | Rapid keyboard navigation (`1`–`5`, `Space`, `M`, `?`, `Esc`) for hands-on presentations |
| **REST API** | Full HTTP API for status, state, queues, accounts, WAL, benchmarks, speed control, and data export |

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

The web dashboard (`web/`) is an interactive mission control console designed specifically for technical demonstrations, academic viva, and systems engineering presentations. It pairs geometric, high-contrast typography (**Plus Jakarta Sans**) with clean tabular numbers and hexadecimal telemetry (**JetBrains Mono**).

### Global Navigation & Keyboard Shortcuts

Press `?` anywhere in the dashboard or click the **[⌨ Shortcuts]** button in the header to open the keyboard cheat sheet:

| Key | Action | Description |
|---|---|---|
| `1` | Switch to Page 1 | **Architecture & Pipeline**: Live interactive flow diagram & CPU dispatcher |
| `2` | Switch to Page 2 | **Scheduled DB ➔ Bank DB**: Account ledger, settlement summary, and transmission log |
| `3` | Switch to Page 3 | **Oscilloscope & Benchmarks**: Tri-channel signal oscilloscope & live benchmark HUD |
| `4` | Switch to Page 4 | **DBMS (2PL & ACID)**: Havender DAG visualizer, lock telemetry, and WAL stream |
| `5` | Switch to Page 5 | **OS Kernel (MLFQ & PCB)**: Execution Gantt strip, queues, and PCB deep inspector |
| `Space` | Pause / Resume | Toggle simulation clock execution |
| `M` | Mode Toggle | Switch between `ADAPTIVE_MDACC` and `STANDARD_BASELINE` |
| `Esc` | Close Modal | Dismiss any active theorem, PCB inspector, or shortcuts modal dialog |

---

### Page 1: Architecture & Execution Pipeline

- **Interactive 5-Stage Execution Pipeline Diagram**: Clickable flowchart depicting end-to-end transaction processing. Clicking any stage opens an in-depth computer science theorem modal:
  1. *Scheduled Ingest DB* — Arranged workload batches, arrival velocity, and priority tagging.
  2. *OS Kernel Scheduler* — MLFQ multi-level queuing with SRTF preemption and anti-starvation aging.
  3. *2PL Concurrency Control* — Strict Two-Phase Locking with Havender monotonic ordering deadlock prevention.
  4. *ACID & Atomicity Engine* — All-or-nothing rollback, balance invariance, and Write-Ahead Log sequencing.
  5. *Core Bank Accounts DB* — 12 managed accounts ledger with live balances and lock status.
- **Live Transaction Milestone Flow Tracer**: Real-time visual progress pill strip tracking in-flight transactions as they advance through `NEW` ➔ `INGEST` ➔ `MLFQ` ➔ `2PL` ➔ `ACID` ➔ `SETTLED`.
- **CPU Core Dispatcher**: Real-time display of the currently executing process, remaining burst ticks, quantum allocation, and memory registers.
- **Guaranteed Non-Empty Invariant**: The CPU Dispatcher and MLFQ queues are guaranteed to remain active and populated at all times for seamless presentation demos.

---

### Page 2: Scheduled DB ➔ Bank DB Ledger

- **Account Category Filter Tabs**: Instantly filter the 12 core bank accounts by category:
  - `ALL` (12 Accounts)
  - `RETAIL` (Accounts 1001–1008, including low-balance test accounts)
  - `CORPORATE` (Accounts 2001–2002, treasury accounts)
  - `ATM VAULT` (Accounts 3001–3002, cash reserve vaults)
- **Settlement Volume Summary Bar**: Dynamic telemetry strip aggregating:
  - *Total Ledger Balance*: Live sum of all accounts (strictly conserved at \$12,300,000.00).
  - *Total Debited*: Cumulative debits executed across all accounts.
  - *Total Credited*: Cumulative credits settled across all accounts.
- **Scheduled Ingest Workload Queue**: Chronologically ordered transaction arrivals awaiting dispatch from the workload generator.
- **Official Bank Database Transmission & Settlement Log**:
  - Verifiable settlement audit stream logging PID, source/destination accounts, amount, 2PL lock verification, ACID balance check, WAL LSN, and before/after balances.
  - **Live Search**: Instant full-text filtering by PID, Account ID, or status.
  - **Status Filters**: Quick toggles for `ALL`, `SETTLED`, and `ROLLED_BACK`.
  - **Dual Export**:
    - **Download JSON**: Fetches server-generated `/api/bank-db-log/export` for machine processing.
    - **Export CSV**: Client-side tabular CSV export formatted for spreadsheet analysis.

---

### Page 3: System Oscilloscope & Live Benchmarks

- **Hardware-Style Oscilloscope Toolbar**:
  - **Channel Toggles**: Selectively enable/disable trace channels:
    - `CH1 TPS` (Neon Green, scale: 0–100 transactions/sec)
    - `CH2 Contention` (Warning Amber, scale: 0–1.0 composite index)
    - `CH3 Abort Rate` (Crimson Red, scale: 0–50% transaction aborts)
  - **Freeze Trace**: Hardware freeze button that pauses the oscilloscope canvas without stopping the simulation, allowing forensic analysis of transient contention spikes.
- **5 Interactive Clickable Live Benchmark Scenarios**:
  - `⚡ Flash Sale`: Injects a burst of 60 rapid UPI micro-transactions targeting retail merchants.
  - `💼 Salary Burst`: Dispatches a heavy corporate payroll batch into Q2 to demonstrate quantum compression and aging promotions.
  - `🔥 Hotspot Contention`: Forces 40 simultaneous transfers to target the same two retail accounts to demonstrate 2PL lock arbitration and backoff.
  - `⚠️ Insufficient Funds`: Dispatches overdraft-triggering transactions against low-balance accounts to demonstrate ACID rollback and balance invariance.
  - `⚔️ Baseline Shootout`: Switches to `STANDARD_BASELINE` (unthrottled, zero backoff) under high contention to provoke abort storms, then returns to `ADAPTIVE_MDACC`.
- **Live Benchmark Execution HUD**: Floating progress display showing active scenario name, elapsed execution timer, and animated progress bar.

---

### Page 4: DBMS Architecture (2PL & ACID)

- **2PL Concurrency Control Telemetry**: Live metrics tracking active locks held, total lock acquisitions (Growing Phase), total lock releases (Shrinking Phase), and average lock hold duration.
- **Havender Monotonic Resource Ordering DAG Visualizer**:
  - Interactive visual Directed Acyclic Graph illustrating strict ascending lock acquisition ($A_{\text{source}} < A_{\text{dest}}$).
  - Graphically demonstrates the mathematical impossibility of circular wait conditions ($P_1 \to P_2 \to P_1$).
- **ACID Pillars & Ledger Invariants**:
  - *Atomicity*: All-or-nothing guarantee with automatic debit/credit rollback.
  - *Consistency*: Global ledger conservation invariant ($\sum_{i=1}^{12} B_i = \$12,300,000.00, \Delta = \$0.00$).
  - *Isolation*: Strict Two-Phase Locking ensuring conflict serializability.
  - *Durability*: Monotonic Write-Ahead Log with persistent Log Sequence Numbers (LSN).
- **Live Write-Ahead Log (WAL) Stream**:
  - Real-time audit log of database operations (`PREPARE`, `DEBIT`, `CREDIT`, `COMMIT`, `ABORT`).
  - **WAL Operation Filter Strip**: Filter WAL records by type (`ALL`, `PREPARE`, `DEBIT`, `CREDIT`, `COMMIT`, `ABORT`).

---

### Page 5: OS Kernel Architecture (MLFQ & PCB)

- **Real-Time CPU Execution Gantt Timeline Strip**:
  - 16-slot sliding window visualizer of CPU time slices.
  - Color-coded by process type: **UPI** (Teal), **ATM** (Indigo), and **SALARY** (Violet).
  - Displays PID badge and burst status in each execution slice.
- **Multi-Level Feedback Queues**:
  - *Queue 1 (High Priority)*: Shortest Remaining Time First (SRTF) for interactive UPI/ATM jobs.
  - *Queue 2 (Low Priority)*: Round Robin with dynamic quantum compression for Salary batches.
  - *Waiting Pool*: Processes awaiting account locks with exponential backoff timers.
  - *Anti-Starvation Aging Engine*: Counter tracking promotions from Q2 to Q1.
- **PCB Deep Inspector Modal**:
  - Click any process in the queues or search by PID to open the inspector dialog.
  - Displays hardware registers (PID, State, Priority, Quantum Remaining, Ticks in Queue, Monotonic Lock Set).
  - Displays financial transfer payload (Source, Destination, Amount).
  - Displays full historical state transition timeline with kernel reason codes.
- **Plain-Language Explanations on Every Block**:
  - Every card and panel includes clear, plain-language summaries explaining the underlying operating system and database concepts.

---

## REST API

| Endpoint | Method | Description |
|---|---|---|
| `/api/status` | `GET` | Tick count, queue sizes, TPS, commits, rollbacks, comparison mode |
| `/api/state` | `GET` | Consolidated simulation state: CPU process, queues, telemetry, ML forecast, 2PL stats, ACID metrics, accounts, WAL |
| `/api/queues` | `GET` | Detailed queue contents (Q1, Q2, waiting pool) |
| `/api/accounts` | `GET` | 12 Bank accounts with live balances, lock status, types, debits, credits |
| `/api/wal?limit=N` | `GET` | Write-Ahead Log records with sequence numbers, operations, balances (default: 30) |
| `/api/scheduled?limit=N` | `GET` | Upcoming scheduled transactions awaiting dispatch from workload (default: 20) |
| `/api/bank-db-log?limit=N` | `GET` | Official Bank Database transmission & settlement records with 2PL/ACID verification (default: 50) |
| `/api/bank-db-log/export` | `GET` | Download official bank database transmission log as `bank_db_transmission_log.json` |
| `/api/benchmark/trigger?scenario=X` | `POST` | Trigger live benchmark scenarios (`flash_sale`, `salary_burst`, `hotspot_contention`, `insufficient_funds`, `baseline_shootout`) |
| `/api/transitions?limit=N` | `GET` | Recent state transition records with detailed kernel reason strings (default: 100) |
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
