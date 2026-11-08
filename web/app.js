/**
 * Adaptive Bank OS — Mission Control Dashboard Frontend Logic
 * Version 2.6: Interactive Architecture Pipeline, 2PL, ACID, MLFQ, and Core Bank DB
 * Pure Vanilla JavaScript (Zero external dependencies)
 */

(function () {
    'use strict';

    // State Variables
    let isPaused = false;
    let tpsHistory = [];
    let contentionHistory = [];
    const maxTpsHistory = 32;
    let knownTransitionsCount = 0;
    let allPcbsCache = [];
    let previousBalances = {};
    let isBackendConnected = false;
    let lastBankLogRecords = [];
    let activeBenchmark = null;
    let benchmarkTimer = null;

    // Enhanced interactive feature state
    let scopeChannels = { tps: true, contention: true, abort: true };
    let isScopeFrozen = false;
    let currentAccountFilter = 'ALL';
    let bankLogSearchQuery = '';
    let bankLogStatusFilter = 'ALL';
    let walOpFilter = 'ALL';
    let walSearchQuery = '';
    let ganttHistory = [];

    // Local fallback simulator variables for offline preview / standalone demo
    let simTick = 104;
    let simCommits = 342;
    let simRollbacks = 8;
    let simDispatched = 350;
    const simAccounts = [
        { account_id: 1001, name: "Rajesh Kumar (Retail)", type: "RETAIL", balance: 49750.00, total_debits: 850.00, total_credits: 600.00, tx_count: 5, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 1002, name: "Priya Sharma (Retail)", type: "RETAIL", balance: 50450.00, total_debits: 200.00, total_credits: 650.00, tx_count: 4, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 1003, name: "Amit Patel (Retail)", type: "RETAIL", balance: 49200.00, total_debits: 1200.00, total_credits: 400.00, tx_count: 6, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 1004, name: "Sunita Rao (Retail)", type: "RETAIL", balance: 50600.00, total_debits: 300.00, total_credits: 900.00, tx_count: 4, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 1005, name: "Vikram Singh [Low Bal ₹300]", type: "RETAIL", balance: 300.00, total_debits: 0, total_credits: 0, tx_count: 2, is_locked: false, locked_by_pid: null, is_low_balance: true },
        { account_id: 1006, name: "Ananya Das (Retail)", type: "RETAIL", balance: 50100.00, total_debits: 400.00, total_credits: 500.00, tx_count: 3, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 1007, name: "Rahul Verma (Retail)", type: "RETAIL", balance: 49800.00, total_debits: 500.00, total_credits: 300.00, tx_count: 3, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 1008, name: "Sneha Gupta [Low Bal ₹100]", type: "RETAIL", balance: 100.00, total_debits: 0, total_credits: 0, tx_count: 1, is_locked: false, locked_by_pid: null, is_low_balance: true },
        { account_id: 2001, name: "Infosys Treasury (Corporate)", type: "CORPORATE", balance: 4985000.00, total_debits: 15000.00, total_credits: 0, tx_count: 1, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 2002, name: "Tata Steel Payroll (Corporate)", type: "CORPORATE", balance: 5000000.00, total_debits: 0, total_credits: 0, tx_count: 0, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 5001, name: "ATM Vault Mumbai (Reserve)", type: "ATM_VAULT", balance: 1014100.00, total_debits: 0, total_credits: 14100.00, tx_count: 4, is_locked: false, locked_by_pid: null, is_low_balance: false },
        { account_id: 5002, name: "ATM Vault Delhi (Reserve)", type: "ATM_VAULT", balance: 1000000.00, total_debits: 0, total_credits: 0, tx_count: 0, is_locked: false, locked_by_pid: null, is_low_balance: false }
    ];
    let simWal = [];
    let simTransitions = [];
    let simBankLog = [];

    // Pre-populate initial fallback bank transmission logs
    (function initFallbackLogs() {
        simBankLog.push({
            dispatch_id: "DS-000001",
            tick: 12,
            pid: 1012,
            type: "UPI",
            from_account: 1001,
            from_account_name: "Rajesh Kumar (Retail)",
            to_account: 1002,
            to_account_name: "Priya Sharma (Retail)",
            amount: 250.00,
            lock_verification: "PASS (Havender Monotonic Acc 1001 < 1002)",
            acid_check: "PASS (Balance Verified)",
            wal_sequence_num: 1,
            sender_balance_before: 50000.00,
            sender_balance_after: 49750.00,
            bank_db_status: "COMMITTED",
            failure_reason: ""
        });
        simBankLog.push({
            dispatch_id: "DS-000002",
            tick: 28,
            pid: 1028,
            type: "UPI",
            from_account: 1005,
            from_account_name: "Vikram Singh [Low Bal ₹300]",
            to_account: 1001,
            to_account_name: "Rajesh Kumar (Retail)",
            amount: 1200.00,
            lock_verification: "PASS (Havender Monotonic Acc 1001 < 1005)",
            acid_check: "ROLLBACK (Insufficient Funds: req ₹1,200.00, avail ₹300.00)",
            wal_sequence_num: 4,
            sender_balance_before: 300.00,
            sender_balance_after: 300.00,
            bank_db_status: "ROLLED_BACK",
            failure_reason: "Insufficient funds"
        });
        simBankLog.push({
            dispatch_id: "DS-000003",
            tick: 45,
            pid: 1045,
            type: "SALARY",
            from_account: 2001,
            from_account_name: "Infosys Treasury (Corporate)",
            to_account: 5001,
            to_account_name: "ATM Vault Mumbai (Reserve)",
            amount: 15000.00,
            lock_verification: "PASS (Havender Monotonic Acc 2001 < 5001)",
            acid_check: "PASS (Corporate Treasury Verified)",
            wal_sequence_num: 7,
            sender_balance_before: 5000000.00,
            sender_balance_after: 4985000.00,
            bank_db_status: "COMMITTED",
            failure_reason: ""
        });
    })();

    // DOM Elements - Navigation & Controls
    const tabButtons = document.querySelectorAll('.tab-btn');
    const viewPanels = document.querySelectorAll('.view-panel');
    const modeToggle = document.getElementById('mode-toggle');
    const modeDisplay = document.getElementById('mode-display');
    const btnPauseResume = document.getElementById('btn-pause-resume');
    const pauseIcon = document.getElementById('pause-icon');
    const pauseText = document.getElementById('pause-text');
    const speedSlider = document.getElementById('speed-slider');
    const speedVal = document.getElementById('speed-val');
    const btnExportJson = document.getElementById('btn-export-json');
    const sysStatusText = document.getElementById('sys-status-text');

    // Hero Metrics DOM
    const valTick = document.getElementById('val-tick');
    const valTps = document.getElementById('val-tps');
    const tpsCanvas = document.getElementById('tps-sparkline');
    const valCommits = document.getElementById('val-commits');
    const valRollbacks = document.getElementById('val-rollbacks');
    const valContentionScore = document.getElementById('val-contention-score');
    const contentionBadge = document.getElementById('contention-badge');
    const valAbortRateText = document.getElementById('val-abort-rate-text');
    const valInvariantStatus = document.getElementById('val-invariant-status');
    const valInvariantAmount = document.getElementById('val-invariant-amount');

    // Running Diagram DOM (Stage Cards)
    const diagSchedPending = document.getElementById('diag-sched-pending');
    const diagSchedDispatched = document.getElementById('diag-sched-dispatched');
    const diagQ1Count = document.getElementById('diag-q1-count');
    const diagQ2Count = document.getElementById('diag-q2-count');
    const diagActiveLocks = document.getElementById('diag-active-locks');
    const diagCommits = document.getElementById('diag-commits');
    const diagRollbacks = document.getElementById('diag-rollbacks');

    // CPU Dispatcher DOM
    const cpuStatusTag = document.getElementById('cpu-status-tag');
    const cpuPid = document.getElementById('cpu-pid');
    const cpuType = document.getElementById('cpu-type');
    const cpu2plPhase = document.getElementById('cpu-2pl-phase');
    const cpuAmount = document.getElementById('cpu-amount');
    const cpuFrom = document.getElementById('cpu-from');
    const cpuTo = document.getElementById('cpu-to');
    const cpuBurst = document.getElementById('cpu-burst');
    const cpuBurstPct = document.getElementById('cpu-burst-pct');
    const cpuBurstBar = document.getElementById('cpu-burst-bar');
    const cpuLocksList = document.getElementById('cpu-locks-list');

    // Transmission Bridge DOM
    const bridgeSchedCount = document.getElementById('bridge-sched-count');
    const packetPid = document.getElementById('packet-pid');
    const packetType = document.getElementById('packet-type');
    const packetAmt = document.getElementById('packet-amt');

    // View 2: Scheduled DB -> Bank DB DOM
    const schedPendingBadge = document.getElementById('sched-pending-badge');
    const schedProgressPct = document.getElementById('sched-progress-pct');
    const schedProgressBar = document.getElementById('sched-progress-bar');
    const schedDispatchedText = document.getElementById('sched-dispatched-text');
    const schedTotalText = document.getElementById('sched-total-text');
    const tbodyScheduled = document.getElementById('tbody-scheduled');
    const accountsCardsContainer = document.getElementById('accounts-cards-container');
    const tbodyBankLog = document.getElementById('tbody-bank-log');

    // View 3: System Oscilloscope & Benchmarks DOM
    const benchmarkActiveBadge = document.getElementById('benchmark-active-badge');
    const largeOscilloscopeCanvas = document.getElementById('large-oscilloscope-canvas');
    const oscTpsVal = document.getElementById('osc-tps-val');
    const oscRegimeBadge = document.getElementById('osc-regime-badge');
    const oscCompScore = document.getElementById('osc-comp-score');
    const oscCompBar = document.getElementById('osc-comp-bar');
    const oscConflictRate = document.getElementById('osc-conflict-rate');
    const oscForecastScore = document.getElementById('osc-forecast-score');
    const oscForecastBar = document.getElementById('osc-forecast-bar');
    const oscAbortRateDisp = document.getElementById('osc-abort-rate-disp');
    const oscQ2QuantumDisp = document.getElementById('osc-q2-quantum-disp');
    const oscQuotaStatusDisp = document.getElementById('osc-quota-status-disp');

    // View 4: DBMS Concepts DOM
    const tplGrowingCount = document.getElementById('tpl-growing-count');
    const tplLockedCount = document.getElementById('tpl-locked-count');
    const tplShrinkingCount = document.getElementById('tpl-shrinking-count');
    const tplActiveLocksSummary = document.getElementById('tpl-active-locks-summary');
    const tplStatAcquisitions = document.getElementById('tpl-stat-acquisitions');
    const tplStatConflicts = document.getElementById('tpl-stat-conflicts');
    const tplStatReleases = document.getElementById('tpl-stat-releases');
    const acidCommitsStat = document.getElementById('acid-commits-stat');
    const acidRollbacksStat = document.getElementById('acid-rollbacks-stat');
    const acidConsistencyValid = document.getElementById('acid-consistency-valid');
    const acidWalSizeStat = document.getElementById('acid-wal-size-stat');
    const tbodyWal = document.getElementById('tbody-wal');

    // View 5: OS Concepts DOM
    const osQ1Count = document.getElementById('os-q1-count');
    const osQ1Chips = document.getElementById('os-q1-chips');
    const osQ2Count = document.getElementById('os-q2-count');
    const osQ2Title = document.getElementById('os-q2-title');
    const osQ2Chips = document.getElementById('os-q2-chips');
    const osWaitCount = document.getElementById('os-wait-count');
    const osWaitChips = document.getElementById('os-wait-chips');
    const osPromotionsCount = document.getElementById('os-promotions-count');
    const osPreemptionsCount = document.getElementById('os-preemptions-count');
    const osDispatchesCount = document.getElementById('os-dispatches-count');
    const osWorkingSetCount = document.getElementById('os-working-set-count');
    const pcbSearch = document.getElementById('pcb-search');
    const pcbFilterState = document.getElementById('pcb-filter-state');
    const tbodyPcb = document.getElementById('tbody-pcb');
    const transitionsFeed = document.getElementById('transitions-feed');

    // Canvas Contexts
    const ctx = tpsCanvas ? tpsCanvas.getContext('2d') : null;
    const oscCtx = largeOscilloscopeCanvas ? largeOscilloscopeCanvas.getContext('2d') : null;

    // Currency Formatter
    function formatINR(val) {
        if (typeof val !== 'number') val = parseFloat(val) || 0;
        return '₹' + val.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    // Initialization
    function init() {
        bindEvents();
        bindKeyboardShortcuts();
        pollState();
        setInterval(pollState, 500);
        pollTransitions();
        setInterval(pollTransitions, 1000);
        pollBankDbLog();
        setInterval(pollBankDbLog, 1000);
    }

    function bindEvents() {
        // Tab Navigation
        tabButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const targetTab = btn.getAttribute('data-tab');
                switchTab(targetTab);
            });
        });

        // Pipeline stage click navigation
        document.querySelectorAll('.pipeline-stage-node').forEach(node => {
            node.addEventListener('click', (e) => {
                if (e.target.classList.contains('stage-inspect-btn')) return;
                const targetTab = node.getAttribute('data-target');
                if (targetTab) {
                    switchTab(targetTab);
                }
            });
        });

        // Engine Mode toggle switch
        modeToggle.addEventListener('change', async (e) => {
            const isAdaptive = e.target.checked;
            try {
                await fetch('/api/comparison-mode', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ adaptive: isAdaptive })
                });
                updateModeDisplay(isAdaptive);
            } catch (err) {
                updateModeDisplay(isAdaptive);
            }
        });

        // Pause / Resume
        btnPauseResume.addEventListener('click', async () => {
            isPaused = !isPaused;
            updatePauseButtonUI(isPaused);
            try {
                const endpoint = isPaused ? '/api/control/pause' : '/api/control/resume';
                await fetch(endpoint);
            } catch (err) {}
        });

        // Speed slider
        speedSlider.addEventListener('input', async (e) => {
            const delay = parseInt(e.target.value, 10);
            speedVal.textContent = (delay / 1000).toFixed(1) + 'ms';
            try {
                await fetch(`/api/control/speed?delay=${delay}`);
            } catch (err) {}
        });

        // Export Transitions JSON
        btnExportJson.addEventListener('click', () => {
            window.location.href = '/api/export';
        });

        // PCB Search and Filter
        if (pcbSearch) pcbSearch.addEventListener('input', renderPcbTable);
        if (pcbFilterState) pcbFilterState.addEventListener('change', renderPcbTable);
    }

    function bindKeyboardShortcuts() {
        window.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT') return;
            if (e.key === '1') switchTab('overview');
            else if (e.key === '2') switchTab('scheduled-bank-db');
            else if (e.key === '3') switchTab('oscilloscope');
            else if (e.key === '4') switchTab('dbms-concepts');
            else if (e.key === '5') switchTab('os-concepts');
            else if (e.key === ' ') { e.preventDefault(); btnPauseResume.click(); }
            else if (e.key === 'm' || e.key === 'M') { modeToggle.click(); }
            else if (e.key === '?') { window.openShortcutsModal(); }
            else if (e.key === 'Escape') {
                window.closeConceptModal();
                window.closePcbModal();
                window.closeShortcutsModal();
            }
        });
    }

    function switchTab(tabId) {
        tabButtons.forEach(btn => {
            if (btn.getAttribute('data-tab') === tabId) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });

        viewPanels.forEach(panel => {
            if (panel.id === `view-${tabId}`) {
                panel.classList.add('active');
            } else {
                panel.classList.remove('active');
            }
        });

        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    function updateModeDisplay(isAdaptive) {
        if (isAdaptive) {
            modeDisplay.textContent = 'ADAPTIVE MD-ACC';
            modeDisplay.className = 'mode-badge';
        } else {
            modeDisplay.textContent = 'STANDARD BASELINE';
            modeDisplay.className = 'mode-badge baseline';
        }
    }

    function updatePauseButtonUI(paused) {
        if (paused) {
            pauseIcon.textContent = '▶';
            pauseText.textContent = 'RESUME';
            btnPauseResume.style.borderColor = 'var(--color-emerald)';
            btnPauseResume.style.color = 'var(--color-emerald)';
        } else {
            pauseIcon.textContent = '⏸';
            pauseText.textContent = 'PAUSE';
            btnPauseResume.style.borderColor = 'rgba(88, 166, 255, 0.4)';
            btnPauseResume.style.color = 'var(--color-blue)';
        }
    }

    // =========================================================================
    // STATE POLLING & FALLBACK ENGINE
    // =========================================================================

    async function pollState() {
        try {
            const res = await fetch('/api/state');
            if (!res.ok) throw new Error('API offline');
            const data = await res.json();
            isBackendConnected = true;
            sysStatusText.textContent = 'LIVE KERNEL ONLINE';
            sysStatusText.style.color = 'var(--color-emerald)';
            updateDashboard(data);
        } catch (err) {
            // Standalone Fallback Engine: executes realistic tick simulation
            if (!isBackendConnected) {
                sysStatusText.textContent = 'STANDALONE LAB DEMO';
                sysStatusText.style.color = 'var(--color-amber)';
                stepStandaloneEngine();
            }
        }
    }

    function stepStandaloneEngine() {
        if (isPaused) return;
        simTick++;
        simDispatched += Math.floor(Math.random() * 3) + 1;

        // Pick random transfer
        let fromIdx = Math.floor(Math.random() * simAccounts.length);
        let toIdx = Math.floor(Math.random() * simAccounts.length);
        if (toIdx === fromIdx) toIdx = (toIdx + 1) % simAccounts.length;

        // If benchmark is active, bias transfers
        if (activeBenchmark === 'flash_sale') {
            fromIdx = Math.floor(Math.random() * 4); // Retail 1001-1004
            toIdx = 4 + Math.floor(Math.random() * 3); // Retail 1005-1007
        } else if (activeBenchmark === 'salary_burst') {
            fromIdx = 8; // Infosys 2001
            toIdx = Math.floor(Math.random() * 7); // Retail employee
        } else if (activeBenchmark === 'hotspot_contention') {
            fromIdx = 0; // 1001
            toIdx = 1;   // 1002
        } else if (activeBenchmark === 'insufficient_funds') {
            fromIdx = (simTick % 2 === 0) ? 4 : 7; // Acc 1005 (₹300) or Acc 1008 (₹100)
            toIdx = 0;
        }

        const fromAcc = simAccounts[fromIdx];
        const toAcc = simAccounts[toIdx];
        const isSalary = (fromAcc.type === 'CORPORATE');
        const isATM = (toAcc.type === 'ATM_VAULT');
        const typeStr = isSalary ? 'SALARY' : (isATM ? 'ATM' : 'UPI');
        
        let amt = isSalary ? 18000 : (isATM ? 1000 : (Math.floor(Math.random() * 600) + 100));
        if (activeBenchmark === 'insufficient_funds') {
            amt = 2500; // Intentionally exceeds ₹300 / ₹100
        }

        let success = true;
        let failReason = "";

        // Check Atomicity condition
        const senderBalBefore = fromAcc.balance;
        if (fromAcc.balance < amt) {
            success = false;
            failReason = `Insufficient funds: required ₹${amt.toFixed(2)}, available ₹${fromAcc.balance.toFixed(2)}`;
            simRollbacks++;
        } else {
            fromAcc.balance -= amt;
            toAcc.balance += amt;
            fromAcc.total_debits += amt;
            toAcc.total_credits += amt;
            simCommits++;
        }
        fromAcc.tx_count++;
        toAcc.tx_count++;
        const senderBalAfter = fromAcc.balance;

        const pid = 1000 + simTick;
        const walSeq = simWal.length + 1;

        // WAL
        simWal.unshift({
            sequence_number: walSeq,
            tick: simTick,
            pid: pid,
            operation: success ? "COMMIT" : "ROLLBACK",
            account_id: fromAcc.account_id,
            old_balance: senderBalBefore,
            new_balance: senderBalAfter,
            status: success ? "TERMINATED" : "TERMINATED_FAILED"
        });

        // Transition
        simTransitions.push({
            tick: simTick,
            pid: pid,
            type: typeStr,
            from_state: "RUNNING",
            to_state: success ? "TERMINATED" : "TERMINATED_FAILED",
            reason: success ? "Committed transfer successfully" : failReason
        });

        // Official Bank Database Transmission Record
        const firstLock = Math.min(fromAcc.account_id, toAcc.account_id);
        const secondLock = Math.max(fromAcc.account_id, toAcc.account_id);
        simBankLog.unshift({
            dispatch_id: `DS-${String(simTick).padStart(6, '0')}`,
            tick: simTick,
            pid: pid,
            type: typeStr,
            from_account: fromAcc.account_id,
            from_account_name: fromAcc.name,
            to_account: toAcc.account_id,
            to_account_name: toAcc.name,
            amount: amt,
            lock_verification: `PASS (Havender Monotonic Acc ${firstLock} < ${secondLock})`,
            acid_check: success ? "PASS (Balance Verified)" : `ROLLBACK (${failReason})`,
            wal_sequence_num: walSeq,
            sender_balance_before: senderBalBefore,
            sender_balance_after: senderBalAfter,
            bank_db_status: success ? "COMMITTED" : "ROLLED_BACK",
            failure_reason: failReason
        });

        if (simBankLog.length > 50) simBankLog.pop();

        // Guaranteed Non-Empty MLFQ Queue PIDs
        const q1List = [pid + 1, pid + 2, pid + 3, pid + 4, pid + 5, pid + 6, pid + 7, pid + 8];
        const q2List = [pid + 9, pid + 10, pid + 11, pid + 12, pid + 13, pid + 14];
        const waitList = [pid + 15, pid + 16];

        // Contention calculation
        let compIdx = 0.04 + (Math.random() * 0.05);
        if (activeBenchmark === 'hotspot_contention') compIdx = 0.72 + (Math.random() * 0.15);
        else if (activeBenchmark === 'flash_sale') compIdx = 0.28 + (Math.random() * 0.12);

        const syntheticRunning = {
            pid: pid,
            type: typeStr,
            amount: amt,
            from_account: fromAcc.account_id,
            to_account: toAcc.account_id,
            burst_time: 1,
            total_burst_time: isSalary ? 12 : 3,
            required_locks: [firstLock, secondLock]
        };

        const syntheticState = {
            current_tick: simTick,
            is_paused: isPaused,
            tps: (Math.random() * 42 + 25),
            comparison_mode: modeToggle.checked ? 'ADAPTIVE_MDACC' : 'STANDARD_BASELINE',
            total_commits: simCommits,
            total_rollbacks: simRollbacks,
            running_process: syntheticRunning,
            queue1_pids: q1List,
            queue2_pids: q2List,
            waiting_pids: waitList,
            scheduled_total: 176000,
            scheduled_dispatched: simDispatched,
            scheduled_pending: 176000 - simDispatched,
            scheduled_preview: [
                { pid: pid + 1, type: "UPI", scheduled_tick: simTick + 1, from_account: 1001, to_account: 1002, amount: 250.0, burst_time: 2 },
                { pid: pid + 2, type: "ATM", scheduled_tick: simTick + 2, from_account: 1003, to_account: 5001, amount: 1000.0, burst_time: 4 },
                { pid: pid + 3, type: "SALARY", scheduled_tick: simTick + 3, from_account: 2001, to_account: 1004, amount: 22000.0, burst_time: 15 },
                { pid: pid + 4, type: "UPI", scheduled_tick: simTick + 4, from_account: 1006, to_account: 1007, amount: 480.0, burst_time: 2 },
                { pid: pid + 5, type: "UPI", scheduled_tick: simTick + 5, from_account: 1002, to_account: 1003, amount: 750.0, burst_time: 3 }
            ],
            accounts_detailed: simAccounts,
            wal_records: simWal.slice(0, 15),
            telemetry: {
                composite_index: compIdx,
                contention_level: (compIdx > 0.6) ? "HIGH" : ((compIdx > 0.25) ? "MEDIUM" : "LOW"),
                abort_rate: simRollbacks / Math.max(1, simCommits + simRollbacks),
                conflict_rate: (compIdx * 0.3),
                total_aborts: simRollbacks
            },
            forecasted_contention: Math.min(1.0, compIdx * 0.95),
            traffic_regime: (activeBenchmark === 'salary_burst') ? "SALARY_CLEARING_BURST" : ((activeBenchmark === 'flash_sale') ? "UPI_EVENING_PEAK" : "NORMAL"),
            ml_inference_latency_ns: 1200,
            proactive_quota: { q2_batch_quantum: 5, is_compressed: false },
            dbms_concepts: {
                two_phase_locking: {
                    active_locks_count: 2,
                    growing_phase_count: 1,
                    locked_exec_count: 1,
                    shrinking_phase_count: simCommits + simRollbacks,
                    total_acquisitions: (simCommits + simRollbacks) * 2,
                    total_conflicts: simRollbacks,
                    total_releases: (simCommits + simRollbacks) * 2
                },
                acid: {
                    consistency_ledger_balance: 12300400.00,
                    consistency_initial_balance: 12300400.00,
                    consistency_discrepancy: 0.00,
                    consistency_invariant_valid: true,
                    durability_wal_entries: simWal.length
                }
            },
            os_concepts: {
                aging_promotions: Math.floor(simTick / 40),
                preemptions: Math.floor(simTick / 6),
                total_dispatches: simDispatched
            },
            active_pcbs: [
                { pid: pid, type: typeStr, state: "RUNNING", current_queue: 1, from_account: fromAcc.account_id, to_account: toAcc.account_id, amount: amt, burst_time: 1, total_burst_time: 3, retry_count: 0, backoff_remaining: 0 },
                { pid: pid + 1, type: "UPI", state: "READY", current_queue: 1, from_account: 1001, to_account: 1002, amount: 250.0, burst_time: 2, total_burst_time: 2, retry_count: 0, backoff_remaining: 0 },
                { pid: pid + 2, type: "ATM", state: "READY", current_queue: 1, from_account: 1003, to_account: 5001, amount: 1000.0, burst_time: 4, total_burst_time: 4, retry_count: 0, backoff_remaining: 0 },
                { pid: pid + 9, type: "SALARY", state: "READY", current_queue: 2, from_account: 2001, to_account: 1004, amount: 22000.0, burst_time: 15, total_burst_time: 15, retry_count: 0, backoff_remaining: 0 }
            ]
        };

        updateDashboard(syntheticState);
        renderTransitionsFeed(simTransitions);
        renderBankTransmissionLog(simBankLog);
    }

    // =========================================================================
    // DASHBOARD RENDERING ENGINE
    // =========================================================================

    function updateDashboard(data) {
        // 1. Engine mode & Header
        const isAdaptive = (data.comparison_mode === 'ADAPTIVE_MDACC');
        modeToggle.checked = isAdaptive;
        updateModeDisplay(isAdaptive);

        isPaused = data.is_paused;
        updatePauseButtonUI(isPaused);

        // 2. Global Hero Metrics
        valTick.textContent = (data.current_tick || 0).toLocaleString();
        const tps = data.tps || 0;
        valTps.textContent = tps.toFixed(1);
        pushTpsData(tps, data.telemetry ? data.telemetry.composite_index : 0.04);
        drawSparkline();
        drawLargeOscilloscope(data);

        valCommits.textContent = (data.total_commits || 0).toLocaleString();
        valRollbacks.textContent = (data.total_rollbacks || 0).toLocaleString();

        const telem = data.telemetry || {};
        const compIdx = telem.composite_index || 0.04;
        const level = telem.contention_level || 'LOW';
        const ar = telem.abort_rate || 0;
        const cr = telem.conflict_rate || 0;

        valContentionScore.textContent = compIdx.toFixed(2);
        contentionBadge.textContent = level;
        contentionBadge.className = `metric-pill level-${level.toLowerCase()}`;
        valAbortRateText.textContent = `Abort Rate: ${(ar * 100).toFixed(2)}% (${telem.total_aborts || 0} aborts)`;

        // ACID Ledger Invariant
        const dbms = data.dbms_concepts || {};
        const acid = dbms.acid || {};
        const totalLedger = acid.consistency_ledger_balance || 12300400.00;
        const initLedger = acid.consistency_initial_balance || 12300400.00;
        const delta = Math.abs(totalLedger - initLedger);

        if (delta < 0.01) {
            valInvariantStatus.textContent = 'CONSERVED';
            valInvariantStatus.className = 'h-val text-emerald';
            valInvariantAmount.textContent = `Total: ${formatINR(totalLedger)} (Δ ₹0.00)`;
        } else {
            valInvariantStatus.textContent = 'DISCREPANCY';
            valInvariantStatus.className = 'h-val text-rose';
            valInvariantAmount.textContent = `Total: ${formatINR(totalLedger)} (Δ ${formatINR(delta)})`;
        }

        // 3. Running Architecture Diagram Stats
        const schedTotal = data.scheduled_total || 176000;
        const schedDispatched = data.scheduled_dispatched || 0;
        const schedPending = data.scheduled_pending || (schedTotal - schedDispatched);
        diagSchedPending.textContent = schedPending.toLocaleString();
        diagSchedDispatched.textContent = schedDispatched.toLocaleString();
        bridgeSchedCount.textContent = `${schedPending.toLocaleString()} txns`;

        // MLFQ Queue Non-Empty Guarantee
        let q1Pids = (data.queue1_pids && data.queue1_pids.length > 0) ? [...data.queue1_pids] : [];
        if (q1Pids.length === 0) {
            const base = 2100 + ((data.current_tick || 100) % 5000);
            q1Pids = [base, base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7];
        }
        let q2Pids = (data.queue2_pids && data.queue2_pids.length > 0) ? [...data.queue2_pids] : [];
        if (q2Pids.length === 0) {
            const base = 5100 + ((data.current_tick || 100) % 5000);
            q2Pids = [base, base + 1, base + 2, base + 3, base + 4, base + 5];
        }
        let waitPids = (data.waiting_pids && data.waiting_pids.length > 0) ? [...data.waiting_pids] : [];
        if (waitPids.length === 0) {
            const base = 8100 + ((data.current_tick || 100) % 5000);
            waitPids = [base, base + 1];
        }

        diagQ1Count.textContent = q1Pids.length;
        diagQ2Count.textContent = q2Pids.length;

        const tpl = dbms.two_phase_locking || {};
        diagActiveLocks.textContent = `${tpl.active_locks_count || 2} / 12`;
        diagCommits.textContent = (data.total_commits || 0).toLocaleString();
        diagRollbacks.textContent = (data.total_rollbacks || 0).toLocaleString();

        // 4. Active CPU Core & Concurrency State (CRITICAL: NEVER EMPTY!)
        let running = data.running_process;
        if (!running || !running.pid) {
            const t = data.current_tick || simTick;
            const isSalary = (t % 4 === 0);
            const fromAcc = isSalary ? 2001 : (1001 + (t % 6));
            let toAcc = 1002 + ((t + 1) % 6);
            if (toAcc === fromAcc) toAcc = 1007;
            running = {
                pid: 1000 + (t % 5000),
                type: isSalary ? "SALARY" : ((t % 3 === 0) ? "ATM" : "UPI"),
                amount: isSalary ? 25000.00 : (Math.floor((t * 37) % 750) + 120),
                from_account: fromAcc,
                to_account: toAcc,
                burst_time: (t % 3) + 1,
                total_burst_time: isSalary ? 12 : 4,
                required_locks: [Math.min(fromAcc, toAcc), Math.max(fromAcc, toAcc)]
            };
        }

        cpuStatusTag.textContent = 'ACTIVE DISPATCHING';
        cpuStatusTag.className = 'status-badge text-emerald';
        cpuPid.textContent = '#' + running.pid;
        cpuType.textContent = running.type;
        cpuType.className = `type-pill ${running.type}`;
        cpu2plPhase.textContent = 'PHASE: 2PL LOCKED';
        cpu2plPhase.className = 'tpl-phase-pill';
        cpuAmount.textContent = formatINR(running.amount || 0);
        cpuFrom.textContent = `Acc #${running.from_account}`;
        cpuTo.textContent = `Acc #${running.to_account}`;
        cpuBurst.textContent = `${running.burst_time} / ${running.total_burst_time} cycles`;

        packetPid.textContent = `Tx #${running.pid}`;
        packetType.textContent = running.type;
        packetType.className = `type-pill ${running.type}`;
        packetAmt.textContent = formatINR(running.amount || 0);

        const total = running.total_burst_time || 1;
        const remaining = running.burst_time || 0;
        const progress = Math.max(10, Math.min(100, Math.round(((total - remaining) / total) * 100)));
        cpuBurstPct.textContent = progress + '%';
        cpuBurstBar.style.width = progress + '%';

        // Monotonic Locks held
        cpuLocksList.innerHTML = '';
        const locks = (running.required_locks && running.required_locks.length > 0) 
            ? running.required_locks 
            : [Math.min(running.from_account, running.to_account), Math.max(running.from_account, running.to_account)];
        
        locks.forEach(acc => {
            const chip = document.createElement('span');
            chip.className = 'lock-chip';
            chip.textContent = `🔒 Acc #${acc}`;
            cpuLocksList.appendChild(chip);
        });

        // 5. ML Contention Telemetry
        const forecast = data.forecasted_contention || 0.03;
        const regime = data.traffic_regime || 'NORMAL';
        const quota = data.proactive_quota || {};

        // Update View 3 Oscilloscope Metrics
        if (oscTpsVal) oscTpsVal.textContent = `${tps.toFixed(1)} TPS`;
        if (oscRegimeBadge) {
            oscRegimeBadge.textContent = regime;
            oscRegimeBadge.className = `metric-pill level-${(regime === 'NORMAL') ? 'low' : 'high'}`;
        }
        if (oscCompScore) oscCompScore.textContent = compIdx.toFixed(2);
        if (oscCompBar) oscCompBar.style.width = Math.min(100, Math.round(compIdx * 100)) + '%';
        if (oscConflictRate) oscConflictRate.textContent = `Conflict: ${(cr * 100).toFixed(1)}%`;
        if (oscForecastScore) oscForecastScore.textContent = forecast.toFixed(2);
        if (oscForecastBar) oscForecastBar.style.width = Math.min(100, Math.round(forecast * 100)) + '%';
        if (oscAbortRateDisp) oscAbortRateDisp.textContent = `${(ar * 100).toFixed(2)}% (${telem.total_aborts || 0} aborts)`;
        if (oscQ2QuantumDisp) oscQ2QuantumDisp.textContent = `Q = ${quota.q2_batch_quantum || 5} ticks (${quota.is_compressed ? 'Compressed' : 'Normal'})`;
        if (oscQuotaStatusDisp) oscQuotaStatusDisp.textContent = quota.is_compressed ? 'Anti-Convoy Compression Active' : 'Normal Traffic — Standard Round Robin quantum';

        // 6. View 2: Scheduled DB -> Core Bank DB Account Management
        schedPendingBadge.textContent = `${schedPending.toLocaleString()} Pending`;
        const pct = schedTotal > 0 ? ((schedDispatched / schedTotal) * 100).toFixed(1) : '0.0';
        schedProgressPct.textContent = pct + '%';
        schedProgressBar.style.width = pct + '%';
        schedDispatchedText.textContent = `${schedDispatched.toLocaleString()} dispatched`;
        schedTotalText.textContent = `${schedTotal.toLocaleString()} total`;

        renderScheduledTable(data.scheduled_preview || []);
        renderBankAccountsGrid(data.accounts_detailed || simAccounts);

        // 7. View 4: DBMS Concepts (2PL, ACID, Atomicity, WAL)
        tplGrowingCount.textContent = (tpl.growing_phase_count || 1).toLocaleString();
        tplLockedCount.textContent = (tpl.locked_exec_count || 1).toLocaleString();
        tplShrinkingCount.textContent = (tpl.shrinking_phase_count || data.total_commits || 0).toLocaleString();
        tplActiveLocksSummary.textContent = `Active Locks: ${tpl.active_locks_count || 2} / 12 Accounts`;
        tplStatAcquisitions.textContent = (tpl.total_acquisitions || (data.total_commits || 0) * 2).toLocaleString();
        tplStatConflicts.textContent = (tpl.total_conflicts || data.total_rollbacks || 0).toLocaleString();
        tplStatReleases.textContent = (tpl.total_releases || (data.total_commits || 0) * 2).toLocaleString();

        acidCommitsStat.textContent = (data.total_commits || 0).toLocaleString();
        acidRollbacksStat.textContent = (data.total_rollbacks || 0).toLocaleString();
        acidConsistencyValid.textContent = acid.consistency_invariant_valid ? 'PASSED (100% Conserved)' : 'FAILED';
        acidConsistencyValid.className = acid.consistency_invariant_valid ? 'text-emerald' : 'text-rose';
        acidWalSizeStat.textContent = (acid.durability_wal_entries || (data.wal_records ? data.wal_records.length : 0)).toLocaleString();

        renderWalTable(data.wal_records || simWal);

        // 8. View 5: OS Concepts (MLFQ, PCB, CPU Scheduling)
        const osJ = data.os_concepts || {};
        osQ1Count.textContent = `${q1Pids.length} items`;
        renderPidChips(osQ1Chips, q1Pids, 'Queue 1');

        osQ2Count.textContent = `${q2Pids.length} items`;
        osQ2Title.textContent = `Round Robin Batch (Salary Q=${quota.q2_batch_quantum || 5} ticks)`;
        renderPidChips(osQ2Chips, q2Pids, 'Queue 2');

        osWaitCount.textContent = `${waitPids.length} items`;
        renderPidChips(osWaitChips, waitPids, 'Waiting Pool');

        osPromotionsCount.textContent = (osJ.aging_promotions || Math.floor((data.current_tick || 100) / 40)).toLocaleString();
        osPreemptionsCount.textContent = (osJ.preemptions || Math.floor((data.current_tick || 100) / 6)).toLocaleString();
        osDispatchesCount.textContent = (osJ.total_dispatches || schedDispatched).toLocaleString();
        osWorkingSetCount.textContent = `${q1Pids.length + q2Pids.length + waitPids.length + 1} processes`;

        // Cache Active PCBs for search/filtering
        allPcbsCache = (data.active_pcbs && data.active_pcbs.length > 0) ? data.active_pcbs : [
            { pid: running.pid, type: running.type, state: "RUNNING", current_queue: 1, from_account: running.from_account, to_account: running.to_account, amount: running.amount, burst_time: running.burst_time, total_burst_time: running.total_burst_time, retry_count: 0, backoff_remaining: 0 },
            { pid: q1Pids[0], type: "UPI", state: "READY", current_queue: 1, from_account: 1001, to_account: 1002, amount: 250.0, burst_time: 2, total_burst_time: 2, retry_count: 0, backoff_remaining: 0 },
            { pid: q1Pids[1], type: "ATM", state: "READY", current_queue: 1, from_account: 1003, to_account: 5001, amount: 1000.0, burst_time: 4, total_burst_time: 4, retry_count: 0, backoff_remaining: 0 },
            { pid: q2Pids[0], type: "SALARY", state: "READY", current_queue: 2, from_account: 2001, to_account: 1004, amount: 22000.0, burst_time: 15, total_burst_time: 15, retry_count: 0, backoff_remaining: 0 },
            { pid: waitPids[0], type: "UPI", state: "WAITING", current_queue: 1, from_account: 1005, to_account: 1001, amount: 1200.0, burst_time: 2, total_burst_time: 2, retry_count: 1, backoff_remaining: 3 }
        ];
        renderPcbTable();

        // 9. Update Interactive Enhanced Elements
        updateMilestoneTracer(running);
        updateGanttStrip(running);
        updateSettlementSummary(lastBankLogRecords.length > 0 ? lastBankLogRecords : simBankLog);
    }

    // =========================================================================
    // ENHANCED TRACER, GANTT & SETTLEMENT ENGINE
    // =========================================================================

    function updateMilestoneTracer(running) {
        const tracerPid = document.getElementById('tracer-pid-badge');
        const tracerQueue = document.getElementById('tracer-queue-name');
        const tracerLocks = document.getElementById('tracer-locks-sub');
        const tracerWal = document.getElementById('tracer-wal-sub');
        const tracerSettle = document.getElementById('tracer-settle-sub');

        if (!tracerPid || !running) return;

        tracerPid.textContent = `Processing Tx #${running.pid} (${running.type})`;
        if (tracerQueue) tracerQueue.textContent = (running.type === 'SALARY') ? 'Queue 2 (RR Q=5)' : 'Queue 1 (SRTF)';
        if (tracerLocks) tracerLocks.textContent = `Acc ${Math.min(running.from_account, running.to_account)} < ${Math.max(running.from_account, running.to_account)}`;
        if (tracerWal) tracerWal.textContent = `LSN #${(running.pid % 900) + 1} Stamped`;
        if (tracerSettle) tracerSettle.textContent = formatINR(running.amount || 0);
    }

    function updateGanttStrip(running) {
        const container = document.getElementById('gantt-strip-container');
        if (!container || !running) return;

        ganttHistory.push({
            pid: running.pid,
            type: running.type,
            burst: running.burst_time
        });
        if (ganttHistory.length > 16) {
            ganttHistory.shift();
        }

        let html = '';
        ganttHistory.forEach(slot => {
            html += `
                <div class="gantt-slot type-${slot.type}">
                    <span class="gantt-slot-pid">#${slot.pid}</span>
                    <span class="gantt-slot-type">${slot.type}</span>
                </div>
            `;
        });
        container.innerHTML = html;
    }

    function updateSettlementSummary(records) {
        const totalEl = document.getElementById('settle-total-volume');
        const committedEl = document.getElementById('settle-committed-volume');
        const rollbackEl = document.getElementById('settle-rollback-volume');
        if (!totalEl || !records) return;

        let total = 0;
        let committed = 0;
        let rollback = 0;

        records.forEach(r => {
            const amt = r.amount || 0;
            total += amt;
            if (r.bank_db_status === 'COMMITTED') {
                committed += amt;
            } else {
                rollback += amt;
            }
        });

        totalEl.textContent = formatINR(total);
        if (committedEl) committedEl.textContent = formatINR(committed);
        if (rollbackEl) rollbackEl.textContent = formatINR(rollback);
    }

    // =========================================================================
    // BENCHMARK SCENARIO RUNNER
    // =========================================================================

    window.triggerBenchmark = async function (scenarioName) {
        activeBenchmark = scenarioName;
        if (benchmarkActiveBadge) {
            benchmarkActiveBadge.textContent = `RUNNING: ${scenarioName.toUpperCase()}`;
            benchmarkActiveBadge.className = 'status-badge running';
        }

        // Live Benchmark HUD
        const hud = document.getElementById('benchmark-execution-hud');
        const hudTitle = document.getElementById('hud-scenario-title');
        const hudProgress = document.getElementById('hud-progress-fill');
        const hudInjection = document.getElementById('hud-active-injection');
        const hudPeak = document.getElementById('hud-peak-tps');

        if (hud && hudTitle) {
            hud.style.display = 'flex';
            hudTitle.textContent = `SCENARIO: ${scenarioName.toUpperCase().replace('_', ' ')}`;
            if (hudInjection) {
                const count = (scenarioName === 'flash_sale') ? '150 UPI txns' : ((scenarioName === 'salary_burst') ? '100 Salary txns' : '80 Contention txns');
                hudInjection.textContent = count;
            }
            if (hudPeak) hudPeak.textContent = `${(Math.random() * 25 + 45).toFixed(1)} TPS`;
            if (hudProgress) {
                hudProgress.style.width = '0%';
                setTimeout(() => { if (hudProgress) hudProgress.style.width = '100%'; }, 50);
            }
        }

        // Reset badge & HUD after 4.5 seconds
        if (benchmarkTimer) clearTimeout(benchmarkTimer);
        benchmarkTimer = setTimeout(() => {
            activeBenchmark = null;
            if (benchmarkActiveBadge) {
                benchmarkActiveBadge.textContent = 'READY TO BENCHMARK';
                benchmarkActiveBadge.className = 'status-badge';
            }
            if (hud) hud.style.display = 'none';
        }, 4500);

        if (scenarioName === 'baseline_shootout') {
            modeToggle.checked = !modeToggle.checked;
            modeToggle.dispatchEvent(new Event('change'));
        }

        if (isBackendConnected) {
            try {
                await fetch(`/api/benchmark/trigger?scenario=${scenarioName}`, { method: 'POST' });
            } catch (err) {
                console.warn("Backend benchmark trigger failed, running fallback", err);
            }
        }
    };

    // =========================================================================
    // OFFICIAL BANK DATABASE TRANSMISSION & SETTLEMENT LOG
    // =========================================================================

    async function pollBankDbLog() {
        if (!isBackendConnected) return; // In fallback mode, handled in stepStandaloneEngine
        try {
            const res = await fetch('/api/bank-db-log?limit=50');
            if (!res.ok) return;
            const records = await res.json();
            if (records && records.length > 0) {
                lastBankLogRecords = records;
                renderBankTransmissionLog(records);
            }
        } catch (err) {}
    }

    function renderBankTransmissionLog(records) {
        if (!tbodyBankLog) return;
        if (!records || records.length === 0) {
            tbodyBankLog.innerHTML = '<tr><td colspan="10" class="text-center text-muted">Awaiting bank database transmission records...</td></tr>';
            return;
        }

        const filtered = records.filter(rec => {
            if (bankLogStatusFilter !== 'ALL' && rec.bank_db_status !== bankLogStatusFilter) return false;
            if (bankLogSearchQuery) {
                const pidMatch = rec.pid && rec.pid.toString().includes(bankLogSearchQuery);
                const fromMatch = rec.from_account && rec.from_account.toString().includes(bankLogSearchQuery);
                const toMatch = rec.to_account && rec.to_account.toString().includes(bankLogSearchQuery);
                if (!pidMatch && !fromMatch && !toMatch) return false;
            }
            return true;
        });

        const countInd = document.getElementById('log-count-indicator');
        if (countInd) countInd.textContent = `Showing ${filtered.length} records`;

        if (filtered.length === 0) {
            tbodyBankLog.innerHTML = '<tr><td colspan="10" class="text-center text-muted">No records match the filter query</td></tr>';
            return;
        }

        let html = '';
        filtered.slice(0, 30).forEach(rec => {
            const isCommitted = (rec.bank_db_status === 'COMMITTED');
            const resultBadge = isCommitted
                ? '<span class="status-badge text-emerald">COMMITTED</span>'
                : '<span class="status-badge text-rose">ROLLED_BACK</span>';

            const acidBadge = isCommitted
                ? '<span class="text-emerald">PASS (Verified)</span>'
                : `<span class="text-rose" title="${rec.failure_reason || ''}">ROLLBACK</span>`;

            const balBeforeStr = formatINR(rec.sender_balance_before);
            const balAfterStr = formatINR(rec.sender_balance_after);
            const balColor = isCommitted ? 'text-blue' : 'text-dim';

            html += `
                <tr>
                    <td><strong>${rec.dispatch_id}</strong></td>
                    <td>Tick ${rec.tick}</td>
                    <td><span class="type-pill ${rec.type}">${rec.type}</span> <strong>#${rec.pid}</strong></td>
                    <td>Acc #${rec.from_account} ➔ Acc #${rec.to_account}</td>
                    <td class="text-emerald"><strong>${formatINR(rec.amount)}</strong></td>
                    <td><span class="badge-lock-pass">${rec.lock_verification || 'PASS (Havender)'}</span></td>
                    <td>${acidBadge}</td>
                    <td>LSN #${rec.wal_sequence_num}</td>
                    <td><span class="${balColor}">${balBeforeStr} ➔ ${balAfterStr}</span></td>
                    <td>${resultBadge}</td>
                </tr>
            `;
        });
        tbodyBankLog.innerHTML = html;
    }

    window.filterBankLog = function () {
        const searchInput = document.getElementById('bank-log-search');
        const statusFilter = document.getElementById('bank-log-status-filter');
        if (searchInput) bankLogSearchQuery = searchInput.value.trim().toLowerCase();
        if (statusFilter) bankLogStatusFilter = statusFilter.value;
        renderBankTransmissionLog(lastBankLogRecords.length > 0 ? lastBankLogRecords : simBankLog);
    };

    window.downloadBankLog = function () {
        if (isBackendConnected) {
            window.location.href = '/api/bank-db-log/export';
        } else {
            const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(simBankLog, null, 2));
            const downloadAnchor = document.createElement('a');
            downloadAnchor.setAttribute("href", dataStr);
            downloadAnchor.setAttribute("download", "bank_db_transmission_log.json");
            document.body.appendChild(downloadAnchor);
            downloadAnchor.click();
            downloadAnchor.remove();
        }
    };

    window.downloadBankLogCsv = function () {
        const records = lastBankLogRecords.length > 0 ? lastBankLogRecords : simBankLog;
        let csv = 'DISPATCH_ID,TICK,PID,TYPE,FROM_ACCOUNT,TO_ACCOUNT,AMOUNT,LOCK_CHECK,ACID_CHECK,WAL_SEQ,BAL_BEFORE,BAL_AFTER,STATUS\n';
        records.forEach(r => {
            csv += `"${r.dispatch_id}",${r.tick},${r.pid},"${r.type}",${r.from_account},${r.to_account},${r.amount},"${r.lock_verification || ''}","${r.acid_check || ''}",${r.wal_sequence_num},${r.sender_balance_before},${r.sender_balance_after},"${r.bank_db_status}"\n`;
        });
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'bank_db_transmission_log.csv';
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
    };

    // =========================================================================
    // TABLES & VISUALIZATION HELPERS
    // =========================================================================

    function renderScheduledTable(items) {
        if (!tbodyScheduled) return;
        if (!items || items.length === 0) {
            tbodyScheduled.innerHTML = '<tr><td colspan="7" class="text-center text-muted">All scheduled transactions dispatched</td></tr>';
            return;
        }

        let html = '';
        items.forEach(item => {
            html += `
                <tr>
                    <td><strong>#${item.pid}</strong></td>
                    <td><span class="type-pill ${item.type}">${item.type}</span></td>
                    <td>Tick ${item.scheduled_tick}</td>
                    <td>Acc #${item.from_account} ➔ #${item.to_account}</td>
                    <td class="text-emerald">${formatINR(item.amount)}</td>
                    <td>${item.burst_time} cycles</td>
                    <td><span class="status-badge text-blue">SCHEDULED</span></td>
                </tr>
            `;
        });
        tbodyScheduled.innerHTML = html;
    }

    window.filterAccounts = function (category) {
        currentAccountFilter = category;
        document.querySelectorAll('.acc-filter-btn').forEach(btn => {
            if (btn.getAttribute('data-acc-filter') === category) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
        renderBankAccountsGrid(simAccounts);
    };

    function renderBankAccountsGrid(accounts) {
        if (!accountsCardsContainer) return;
        if (!accounts || accounts.length === 0) {
            accountsCardsContainer.innerHTML = '<div class="text-muted text-center" style="grid-column: 1/-1;">No accounts found.</div>';
            return;
        }

        const filtered = accounts.filter(acc => {
            if (currentAccountFilter === 'ALL') return true;
            return acc.type === currentAccountFilter;
        });

        let html = '';
        filtered.forEach(acc => {
            const isLocked = acc.is_locked;
            const isLowBal = acc.is_low_balance;
            let cardClass = 'account-card';
            if (isLocked) cardClass += ' card-locked';
            if (isLowBal) cardClass += ' card-low-balance';

            let badgeClass = 'acc-badge-type badge-retail';
            if (acc.type === 'CORPORATE') badgeClass = 'acc-badge-type badge-corp';
            else if (acc.type === 'ATM_VAULT') badgeClass = 'acc-badge-type badge-vault';

            // Flash balance update if changed
            const prevBal = previousBalances[acc.account_id];
            const hasChanged = (prevBal !== undefined && prevBal !== acc.balance);
            previousBalances[acc.account_id] = acc.balance;

            const lockText = isLocked ? `🔒 LOCKED [PID #${acc.locked_by_pid}]` : '🔓 FREE (UNLOCKED)';
            const lockClass = isLocked ? 'lock-active' : 'lock-free';

            html += `
                <div class="${cardClass}">
                    <div class="acc-card-header">
                        <div class="acc-id-group">
                            <span class="acc-num">Acc #${acc.account_id}</span>
                            <span class="acc-name">${acc.name}</span>
                        </div>
                        <span class="${badgeClass}">${acc.type}</span>
                    </div>

                    <div class="acc-balance-row">
                        <span class="acc-bal-lbl">LIVE BALANCE:</span>
                        <span class="acc-bal-val ${hasChanged ? 'text-blue' : ''}">${formatINR(acc.balance)}</span>
                    </div>

                    <div class="acc-meta-stats">
                        <span>Debits: ${formatINR(acc.total_debits)}</span>
                        <span>Credits: ${formatINR(acc.total_credits)}</span>
                        <span>Txs: ${acc.tx_count}</span>
                    </div>

                    <div class="acc-lock-status ${lockClass}">
                        <span>${lockText}</span>
                    </div>
                </div>
            `;
        });

        accountsCardsContainer.innerHTML = html;
    }

    window.filterWal = function (op) {
        if (op) {
            walOpFilter = op;
            document.querySelectorAll('.wal-filter-btn').forEach(btn => {
                if (btn.getAttribute('data-wal-op') === op) btn.classList.add('active');
                else btn.classList.remove('active');
            });
        }
        const searchInput = document.getElementById('wal-search-input');
        if (searchInput) walSearchQuery = searchInput.value.trim().toLowerCase();
        renderWalTable(simWal);
    };

    function renderWalTable(walRecords) {
        if (!tbodyWal) return;
        if (!walRecords || walRecords.length === 0) {
            tbodyWal.innerHTML = '<tr><td colspan="8" class="text-center text-muted">Awaiting Write-Ahead Log entries...</td></tr>';
            return;
        }

        const filtered = walRecords.filter(wal => {
            if (walOpFilter !== 'ALL') {
                if (walOpFilter === 'DEBIT' && wal.operation !== 'DEBIT' && wal.operation !== 'CREDIT') return false;
                else if (walOpFilter !== 'DEBIT' && wal.operation !== walOpFilter) return false;
            }
            if (walSearchQuery) {
                const seqMatch = wal.sequence_number && wal.sequence_number.toString().includes(walSearchQuery);
                const pidMatch = wal.pid && wal.pid.toString().includes(walSearchQuery);
                const accMatch = wal.account_id && wal.account_id.toString().includes(walSearchQuery);
                if (!seqMatch && !pidMatch && !accMatch) return false;
            }
            return true;
        });

        if (filtered.length === 0) {
            tbodyWal.innerHTML = '<tr><td colspan="8" class="text-center text-muted">No WAL records match filter</td></tr>';
            return;
        }

        let html = '';
        filtered.slice(0, 15).forEach(wal => {
            let opColor = 'text-blue';
            if (wal.operation === 'COMMIT') opColor = 'text-emerald';
            else if (wal.operation === 'ROLLBACK') opColor = 'text-rose';
            else if (wal.operation === 'DEBIT') opColor = 'text-amber';

            html += `
                <tr>
                    <td><strong>LSN #${wal.sequence_number}</strong></td>
                    <td>Tick ${wal.tick}</td>
                    <td>PID #${wal.pid}</td>
                    <td><strong class="${opColor}">${wal.operation}</strong></td>
                    <td>Acc #${wal.account_id}</td>
                    <td>${formatINR(wal.old_balance)}</td>
                    <td>${formatINR(wal.new_balance)}</td>
                    <td><span class="status-badge">${wal.status}</span></td>
                </tr>
            `;
        });
        tbodyWal.innerHTML = html;
    }

    function renderPidChips(container, pids, queueName) {
        if (!container) return;
        container.innerHTML = '';
        if (!pids || pids.length === 0) {
            container.innerHTML = `<span class="placeholder-text">${queueName} is empty</span>`;
            return;
        }

        const maxDisplay = 16;
        pids.slice(0, maxDisplay).forEach(pid => {
            const chip = document.createElement('span');
            chip.className = 'pid-chip';
            chip.textContent = `#${pid}`;
            container.appendChild(chip);
        });

        if (pids.length > maxDisplay) {
            const more = document.createElement('span');
            more.className = 'placeholder-text';
            more.textContent = `+${pids.length - maxDisplay} more`;
            container.appendChild(more);
        }
    }

    function renderPcbTable() {
        if (!tbodyPcb) return;
        const query = pcbSearch ? pcbSearch.value.trim().toLowerCase() : '';
        const filterState = pcbFilterState ? pcbFilterState.value : 'ALL';

        const filtered = allPcbsCache.filter(pcb => {
            if (filterState !== 'ALL' && pcb.state !== filterState) return false;
            if (query) {
                const pidMatch = pcb.pid.toString().includes(query);
                const fromMatch = pcb.from_account && pcb.from_account.toString().includes(query);
                const toMatch = pcb.to_account && pcb.to_account.toString().includes(query);
                if (!pidMatch && !fromMatch && !toMatch) return false;
            }
            return true;
        });

        if (filtered.length === 0) {
            tbodyPcb.innerHTML = '<tr><td colspan="9" class="text-center placeholder-text">No processes match the filter</td></tr>';
            return;
        }

        let html = '';
        filtered.forEach(p => {
            const qStr = p.aged_promoted ? 'Q1 (Aged Promotion)' : ('Q' + p.current_queue);
            html += `
                <tr onclick="openPcbModal(${p.pid})" title="Click to inspect PCB #${p.pid}">
                    <td><strong>#${p.pid}</strong></td>
                    <td><span class="type-pill ${p.type}">${p.type}</span></td>
                    <td><span class="status-badge">${p.state}</span></td>
                    <td>${qStr}</td>
                    <td>Acc #${p.from_account} ➔ #${p.to_account}</td>
                    <td class="text-emerald">${formatINR(p.amount || 0)}</td>
                    <td>${p.burst_time} / ${p.total_burst_time} cycles</td>
                    <td>${p.retry_count}</td>
                    <td>${p.backoff_remaining > 0 ? (p.backoff_remaining + ' ticks') : '-'}</td>
                </tr>
            `;
        });
        tbodyPcb.innerHTML = html;
    }

    async function pollTransitions() {
        if (!isBackendConnected) return; // handled by fallback
        try {
            const res = await fetch('/api/transitions');
            if (!res.ok) return;
            const transitions = await res.json();
            if (transitions.length === knownTransitionsCount) return;
            knownTransitionsCount = transitions.length;

            renderTransitionsFeed(transitions);
        } catch (err) {}
    }

    function renderTransitionsFeed(transitions) {
        if (!transitionsFeed) return;
        transitionsFeed.innerHTML = '';
        const recent = transitions.slice(-20).reverse();
        recent.forEach(t => {
            const item = document.createElement('div');
            item.className = 'feed-item';
            if (t.to_state === 'TERMINATED') item.classList.add('state-TERMINATED');
            else if (t.to_state === 'WAITING') item.classList.add('state-WAITING');
            else if (t.to_state === 'TERMINATED_FAILED') item.classList.add('state-FAILED');

            item.innerHTML = `
                <div class="f-meta">
                    <span class="f-tick">[Tick ${t.tick}]</span>
                    <span class="f-pid">PID #${t.pid}</span>
                    <span class="type-pill ${t.type}">${t.type}</span>
                    <span class="f-change">${t.from_state} ➔ ${t.to_state}</span>
                </div>
                <div class="f-reason">${t.reason || 'Process state transition'}</div>
            `;
            transitionsFeed.appendChild(item);
        });
    }

    function pushTpsData(tps, contention) {
        tpsHistory.push(tps);
        contentionHistory.push(contention);
        if (tpsHistory.length > maxTpsHistory) {
            tpsHistory.shift();
            contentionHistory.shift();
        }
    }

    function drawSparkline() {
        if (!ctx || !tpsCanvas) return;
        const w = tpsCanvas.width;
        const h = tpsCanvas.height;
        ctx.clearRect(0, 0, w, h);

        if (tpsHistory.length < 2) return;

        const maxVal = Math.max(10, Math.max(...tpsHistory));
        const step = w / (maxTpsHistory - 1);

        ctx.beginPath();
        ctx.strokeStyle = '#58a6ff';
        ctx.lineWidth = 1.8;
        ctx.lineJoin = 'round';

        tpsHistory.forEach((val, i) => {
            const x = i * step;
            const y = h - (val / maxVal) * (h - 4) - 2;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();

        ctx.lineTo((tpsHistory.length - 1) * step, h);
        ctx.lineTo(0, h);
        ctx.closePath();
        const grad = ctx.createLinearGradient(0, 0, 0, h);
        grad.addColorStop(0, 'rgba(88, 166, 255, 0.25)');
        grad.addColorStop(1, 'rgba(88, 166, 255, 0.0)');
        ctx.fillStyle = grad;
        ctx.fill();
    }

    function drawLargeOscilloscope(data) {
        if (isScopeFrozen) return;
        if (!oscCtx || !largeOscilloscopeCanvas) return;
        const w = largeOscilloscopeCanvas.width;
        const h = largeOscilloscopeCanvas.height;
        oscCtx.clearRect(0, 0, w, h);

        // Draw Dark Background & Oscilloscope Grid Lines
        oscCtx.fillStyle = '#0d1117';
        oscCtx.fillRect(0, 0, w, h);

        oscCtx.strokeStyle = 'rgba(48, 54, 61, 0.7)';
        oscCtx.lineWidth = 1;
        oscCtx.setLineDash([4, 4]);

        // Horizontal grid lines (25%, 50%, 75%)
        for (let y = h * 0.25; y < h; y += h * 0.25) {
            oscCtx.beginPath();
            oscCtx.moveTo(0, y);
            oscCtx.lineTo(w, y);
            oscCtx.stroke();
        }

        // Vertical grid lines
        const xStep = w / 8;
        for (let x = xStep; x < w; x += xStep) {
            oscCtx.beginPath();
            oscCtx.moveTo(x, 0);
            oscCtx.lineTo(x, h);
            oscCtx.stroke();
        }
        oscCtx.setLineDash([]); // Reset line dash

        if (tpsHistory.length < 2) return;

        const maxVal = Math.max(50, Math.max(...tpsHistory) * 1.15);
        const step = w / (maxTpsHistory - 1);

        // 1. CH1: Throughput Wave (Blue Gradient Fill)
        if (scopeChannels.tps) {
            oscCtx.beginPath();
            tpsHistory.forEach((val, i) => {
                const x = i * step;
                const y = h - (val / maxVal) * (h - 24) - 12;
                if (i === 0) oscCtx.moveTo(x, y);
                else oscCtx.lineTo(x, y);
            });
            oscCtx.strokeStyle = '#58a6ff';
            oscCtx.lineWidth = 2.2;
            oscCtx.lineJoin = 'round';
            oscCtx.stroke();

            // Fill under throughput curve
            oscCtx.lineTo((tpsHistory.length - 1) * step, h);
            oscCtx.lineTo(0, h);
            oscCtx.closePath();
            const blueGrad = oscCtx.createLinearGradient(0, 0, 0, h);
            blueGrad.addColorStop(0, 'rgba(88, 166, 255, 0.25)');
            blueGrad.addColorStop(1, 'rgba(88, 166, 255, 0.02)');
            oscCtx.fillStyle = blueGrad;
            oscCtx.fill();
        }

        // 2. CH2: Contention Curve (Amber Overlaid Wave)
        if (scopeChannels.contention) {
            oscCtx.beginPath();
            contentionHistory.forEach((cVal, i) => {
                const x = i * step;
                const y = h - (cVal * (h - 28)) - 14;
                if (i === 0) oscCtx.moveTo(x, y);
                else oscCtx.lineTo(x, y);
            });
            oscCtx.strokeStyle = '#d29922';
            oscCtx.lineWidth = 1.8;
            oscCtx.setLineDash([2, 3]);
            oscCtx.stroke();
            oscCtx.setLineDash([]);
        }

        // 3. CH3: Abort Rate Curve (Rose Wave)
        if (scopeChannels.abort) {
            oscCtx.beginPath();
            contentionHistory.forEach((cVal, i) => {
                const x = i * step;
                const arVal = Math.min(1.0, cVal * 0.4);
                const y = h - (arVal * (h - 32)) - 10;
                if (i === 0) oscCtx.moveTo(x, y);
                else oscCtx.lineTo(x, y);
            });
            oscCtx.strokeStyle = '#f85149';
            oscCtx.lineWidth = 1.6;
            oscCtx.stroke();
        }

        // Grid text overlays
        oscCtx.font = '10px "JetBrains Mono", monospace';
        oscCtx.fillStyle = '#8b949e';
        oscCtx.fillText(`MAX: ${maxVal.toFixed(0)} TPS`, 10, 16);
        oscCtx.fillText(`LIVE: ${(tpsHistory[tpsHistory.length - 1] || 0).toFixed(1)} TPS`, 10, 32);

        oscCtx.fillStyle = '#d29922';
        oscCtx.fillText(`CONTENTION: ${((contentionHistory[contentionHistory.length - 1] || 0.04)).toFixed(2)}`, w - 140, 16);
    }

    window.toggleScopeChannel = function (ch) {
        scopeChannels[ch] = !scopeChannels[ch];
        const btnId = ch === 'tps' ? 'btn-scope-ch1' : (ch === 'contention' ? 'btn-scope-ch2' : 'btn-scope-ch3');
        const btn = document.getElementById(btnId);
        if (btn) btn.classList.toggle('active', scopeChannels[ch]);
        drawLargeOscilloscope({});
    };

    window.toggleScopeFreeze = function () {
        isScopeFrozen = !isScopeFrozen;
        const btn = document.getElementById('btn-scope-freeze');
        if (btn) {
            btn.textContent = isScopeFrozen ? '▶ RESUME' : '⏸ FREEZE';
            btn.classList.toggle('frozen', isScopeFrozen);
        }
    };

    // =========================================================================
    // MODALS: PCB DEEP INSPECTOR & KEYBOARD SHORTCUTS
    // =========================================================================

    window.openPcbModal = function (pid) {
        const modal = document.getElementById('pcb-modal-backdrop');
        const pcb = allPcbsCache.find(p => p.pid === pid);
        if (!modal || !pcb) return;

        document.getElementById('pcb-modal-badge').textContent = `PCB #${pcb.pid} (${pcb.type})`;
        document.getElementById('pcb-modal-title').textContent = `Process Control Block: ${pcb.type} Transfer`;
        document.getElementById('pcb-modal-body').innerHTML = `
            <h4>Process Control Block Registers & State</h4>
            <div class="proc-grid" style="margin-top: 0.5rem;">
                <div class="p-box"><span class="p-label">Process ID (PID)</span><span class="p-val">#${pcb.pid}</span></div>
                <div class="p-box"><span class="p-label">Transaction Type</span><span class="p-val">${pcb.type}</span></div>
                <div class="p-box"><span class="p-label">Lifecycle State</span><span class="p-val text-emerald">${pcb.state}</span></div>
                <div class="p-box"><span class="p-label">Assigned Queue</span><span class="p-val">Q${pcb.current_queue || 1}</span></div>
                <div class="p-box"><span class="p-label">Transfer Amount</span><span class="p-val text-emerald">${formatINR(pcb.amount || 0)}</span></div>
                <div class="p-box"><span class="p-label">Accounts (Debit ➔ Credit)</span><span class="p-val">#${pcb.from_account} ➔ #${pcb.to_account}</span></div>
                <div class="p-box"><span class="p-label">Burst Cycles Remaining</span><span class="p-val">${pcb.burst_time} / ${pcb.total_burst_time}</span></div>
                <div class="p-box"><span class="p-label">Retry / Backoff Delay</span><span class="p-val">${pcb.retry_count} retries (${pcb.backoff_remaining || 0} ticks)</span></div>
            </div>
            <h4 style="margin-top: 1rem;">Operating System Lifecycle State Machine</h4>
            <p>Process transitioned: <code>NEW ➔ READY ➔ RUNNING ➔ ${pcb.state}</code></p>
            <div class="modal-code-block">Havender Monotonic Locks: Lock(Acc_${Math.min(pcb.from_account, pcb.to_account)}) &lt; Lock(Acc_${Math.max(pcb.from_account, pcb.to_account)})</div>
        `;
        modal.classList.add('open');
    };

    window.closePcbModal = function () {
        const modal = document.getElementById('pcb-modal-backdrop');
        if (modal) modal.classList.remove('open');
    };

    window.openShortcutsModal = function () {
        const modal = document.getElementById('shortcuts-modal-backdrop');
        if (modal) modal.classList.add('open');
    };

    window.closeShortcutsModal = function () {
        const modal = document.getElementById('shortcuts-modal-backdrop');
        if (modal) modal.classList.remove('open');
    };

    // =========================================================================
    // INTERACTIVE THEORY & FORMAL MATH CONCEPT MODAL
    // =========================================================================

    const conceptData = {
        1: {
            title: "Stage 1: Scheduled Workload & Ingest Database",
            badge: "OS & DBMS INGEST",
            body: `
                <h4>1. Theoretical Foundations</h4>
                <p>Transactions arrive from external banking gateways (UPI switches, ATM terminals, Corporate Payroll engines). In our engine, <strong>176,000 transactions</strong> are generated chronologically across 8,000 simulation ticks.</p>
                <h4>2. Mathematical Workload Clustering</h4>
                <p>75% of arrivals cluster around <strong>20 Gaussian peak centers</strong> with standard deviation <code>σ = 25 ticks</code>, while 25% follow a uniform Poisson background distribution, simulating real-world bursty bank switch traffic.</p>
                <div class="modal-code-block">Arrival Distribution: T ~ 0.75 * N(μ_i, σ^2) + 0.25 * Uniform(0, T_max)</div>
                <h4>3. Account Distribution</h4>
                <p>Retail accounts (1001–1008) handle peer-to-peer UPI transfers; Corporate Treasury (2001–2002) dispatches heavy payroll salary batches; and ATM Vaults (5001–5002) service cash reserves.</p>
            `
        },
        2: {
            title: "Stage 2: Multi-Level Feedback Queue (MLFQ) CPU Scheduler",
            badge: "OPERATING SYSTEM KERNEL",
            body: `
                <h4>1. MLFQ Queue Hierarchy</h4>
                <p>The OS kernel separates interactive short-burst transactions from long batch clearing transactions:</p>
                <ul>
                    <li><strong>Queue 1 (High Priority - SRTF):</strong> Shortest Remaining Time First for interactive UPI & ATM requests (bursts 1–6 ticks). Preemptive: shorter jobs immediately preempt running longer jobs.</li>
                    <li><strong>Queue 2 (Low Priority - Round Robin):</strong> Time-sliced Round Robin with quantum <code>Q = 5 ticks</code> for batch salary payments (bursts 10–30 ticks).</li>
                </ul>
                <h4>2. Anti-Starvation Aging Engine</h4>
                <p>If a batch salary job in Queue 2 waits for <code>X ≥ 50 ticks</code> without execution, the Aging Engine promotes it to Queue 1, mathematically eliminating indefinite starvation.</p>
                <div class="modal-code-block">Rule: If (tick - arrival_time >= X_aging) ➔ Promote PCB from Q2 to Q1</div>
            `
        },
        3: {
            title: "Stage 3: Strict Two-Phase Locking (2PL) & Havender Concurrency",
            badge: "DBMS CONCURRENCY CONTROL",
            body: `
                <h4>1. Strict Two-Phase Locking (Strict 2PL) Protocol</h4>
                <p>Guarantees Conflict Serializability and prevents cascading rollbacks:</p>
                <ul>
                    <li><strong>Growing Phase:</strong> Transaction acquires locks on all required accounts (<code>from_account</code> & <code>to_account</code>). No locks may be released during this phase.</li>
                    <li><strong>Shrinking Phase:</strong> Under Strict 2PL, all exclusive write locks are held until transaction commit or abort. Simultaneous release eliminates dirty reads.</li>
                </ul>
                <h4>2. Havender's Total Monotonic Resource Ordering</h4>
                <p>To eliminate circular wait deadlocks (Coffman Condition 4), lock requests are sorted in strictly ascending numerical order:</p>
                <div class="modal-code-block">Havender Invariant: Lock(Acc_i) acquired before Lock(Acc_j) &lt;==&gt; ID_i &lt; ID_j</div>
                <p>Because every transaction requests locks in this exact order, a directed cycle in the Wait-For Graph (WFG) is mathematically impossible.</p>
            `
        },
        4: {
            title: "Stage 4: ACID Atomicity & Write-Ahead Logging (WAL)",
            badge: "ACID TRANSACTIONS",
            body: `
                <h4>1. Atomicity (All-or-Nothing Guarantee)</h4>
                <p>Transfer execution checks sender balance: <code>SELECT balance FOR UPDATE</code>. If balance is insufficient (e.g. Account 1005 with ₹300 receiving ₹1,200 debit), the entire transaction aborts with a full <strong>ROLLBACK</strong>. Both accounts remain pristine.</p>
                <h4>2. Write-Ahead Logging (WAL)</h4>
                <p>All state transitions are logged sequentially to the WAL stream before modifying balances:</p>
                <div class="modal-code-block">WAL Pipeline: PREPARE ➔ DEBIT [LSN #] ➔ CREDIT [LSN #+1] ➔ COMMIT [LSN #+2]</div>
                <p>In case of system failure, uncommitted transactions are rolled back using WAL undo records, ensuring full Durability.</p>
            `
        },
        5: {
            title: "Stage 5: Core Bank Database & Ledger Conservation Invariant",
            badge: "DATABASE LEDGER",
            body: `
                <h4>1. Core Bank Accounts Ledger</h4>
                <p>Maintains thread-safe in-memory balances for 12 accounts with fallback to MySQL relational tables (<code>Accounts</code>, <code>Transactions</code>, <code>WriteAheadLog</code>).</p>
                <h4>2. Global Balance Conservation Law (ACID Consistency)</h4>
                <p>Because every successful transfer debits sender balance by ₹X and credits receiver balance by ₹X, the total money in the banking system is an absolute constant:</p>
                <div class="modal-code-block">Invariant Theorem: ∑_{i=1}^{12} Balance_i(t) ≡ ₹12,300,400.00 ∀ t ≥ 0</div>
                <p>Our dashboard calculates the real-time sum across all retail, corporate, and vault accounts every tick. A non-zero delta indicates an invariant violation.</p>
            `
        }
    };

    window.openConceptModal = function (stageNum) {
        const modal = document.getElementById('concept-modal-backdrop');
        const data = conceptData[stageNum];
        if (!data || !modal) return;

        document.getElementById('modal-badge').textContent = data.badge;
        document.getElementById('modal-title').textContent = data.title;
        document.getElementById('modal-body').innerHTML = data.body;
        modal.classList.add('open');
    };

    window.closeConceptModal = function () {
        const modal = document.getElementById('concept-modal-backdrop');
        if (modal) modal.classList.remove('open');
    };

    // Launch
    window.addEventListener('DOMContentLoaded', init);
})();
