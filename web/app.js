/**
 * Adaptive Bank OS — Mission Control Dashboard Frontend Logic
 * Pure Vanilla JavaScript (Zero external dependencies)
 * Polling /api/state every 500ms
 */

(function () {
    'use strict';

    // State Variables
    let isPaused = false;
    let tpsHistory = [];
    const maxTpsHistory = 30;
    let knownTransitionsCount = 0;
    let allPcbsCache = [];

    // DOM Elements
    const modeToggle = document.getElementById('mode-toggle');
    const modeDisplay = document.getElementById('mode-display');
    const btnPauseResume = document.getElementById('btn-pause-resume');
    const pauseIcon = document.getElementById('pause-icon');
    const pauseText = document.getElementById('pause-text');
    const speedSlider = document.getElementById('speed-slider');
    const speedVal = document.getElementById('speed-val');
    const btnExportJson = document.getElementById('btn-export-json');

    // Metrics DOM
    const valTps = document.getElementById('val-tps');
    const tpsCanvas = document.getElementById('tps-sparkline');
    const valContentionScore = document.getElementById('val-contention-score');
    const contentionBadge = document.getElementById('contention-badge');
    const contentionBar = document.getElementById('contention-bar');
    const valAbortRate = document.getElementById('val-abort-rate');
    const valAbortSub = document.getElementById('val-abort-sub');
    const abortBar = document.getElementById('abort-bar');
    const valCommits = document.getElementById('val-commits');
    const valRollbacks = document.getElementById('val-rollbacks');
    const valTick = document.getElementById('val-tick');
    const regimeBadge = document.getElementById('regime-badge');
    const valForecast = document.getElementById('val-forecast');
    const valMlLatency = document.getElementById('val-ml-latency');
    const valQuotaStatus = document.getElementById('val-quota-status');

    // CPU DOM
    const cpuStatusTag = document.getElementById('cpu-status-tag');
    const cpuPid = document.getElementById('cpu-pid');
    const cpuType = document.getElementById('cpu-type');
    const cpuAmount = document.getElementById('cpu-amount');
    const cpuFrom = document.getElementById('cpu-from');
    const cpuTo = document.getElementById('cpu-to');
    const cpuBurst = document.getElementById('cpu-burst');
    const cpuBurstPct = document.getElementById('cpu-burst-pct');
    const cpuBurstBar = document.getElementById('cpu-burst-bar');
    const cpuLocksList = document.getElementById('cpu-locks-list');

    // MLFQ DOM
    const q1Count = document.getElementById('q1-count');
    const q1Container = document.getElementById('q1-pids-container');
    const q2Count = document.getElementById('q2-count');
    const q2Container = document.getElementById('q2-pids-container');
    const q2QuantumLabel = document.getElementById('q2-quantum-label');
    const waitCount = document.getElementById('wait-count');
    const waitContainer = document.getElementById('wait-pids-container');

    // PCB Table DOM
    const pcbSearchInput = document.getElementById('pcb-search-input');
    const pcbFilterStatus = document.getElementById('pcb-filter-status');
    const pcbTbody = document.getElementById('pcb-tbody');

    // Transitions Feed DOM
    const transitionsFeed = document.getElementById('transitions-feed');

    // Canvas Context for Sparkline
    const ctx = tpsCanvas.getContext('2d');

    // Initialization
    function init() {
        bindEvents();
        pollState();
        setInterval(pollState, 500);
        pollTransitions();
        setInterval(pollTransitions, 1000);
    }

    function bindEvents() {
        // Mode toggle switch
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
                console.error('Failed to toggle mode:', err);
            }
        });

        // Pause / Resume
        btnPauseResume.addEventListener('click', async () => {
            try {
                const endpoint = isPaused ? '/api/control/resume' : '/api/control/pause';
                await fetch(endpoint);
                isPaused = !isPaused;
                updatePauseButtonUI(isPaused);
            } catch (err) {
                console.error('Failed to pause/resume:', err);
            }
        });

        // Speed slider
        speedSlider.addEventListener('input', async (e) => {
            const delay = parseInt(e.target.value, 10);
            speedVal.textContent = (delay / 1000).toFixed(1) + 'ms';
            try {
                await fetch(`/api/control/speed?delay=${delay}`);
            } catch (err) {
                console.error('Failed to set speed:', err);
            }
        });

        // Export JSON
        btnExportJson.addEventListener('click', () => {
            window.location.href = '/api/export';
        });

        // Filter events
        pcbSearchInput.addEventListener('input', renderPcbTable);
        pcbFilterStatus.addEventListener('change', renderPcbTable);
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
            btnPauseResume.style.borderColor = 'rgba(6, 182, 212, 0.5)';
            btnPauseResume.style.color = 'var(--color-cyan)';
        }
    }

    async function pollState() {
        try {
            const res = await fetch('/api/state');
            if (!res.ok) return;
            const data = await res.json();
            updateUI(data);
        } catch (err) {
            // Server might be restarting
        }
    }

    function updateUI(data) {
        // 1. Header & Engine Mode
        const isAdaptive = (data.comparison_mode === 'ADAPTIVE_MDACC');
        modeToggle.checked = isAdaptive;
        updateModeDisplay(isAdaptive);

        isPaused = data.is_paused;
        updatePauseButtonUI(isPaused);

        // 2. Metrics Panel
        const tps = data.tps || 0;
        valTps.textContent = tps.toFixed(1);
        pushTpsData(tps);
        drawSparkline();

        valTick.textContent = data.current_tick;
        valCommits.textContent = (data.total_commits || 0).toLocaleString();
        valRollbacks.textContent = (data.total_rollbacks || 0).toLocaleString();

        const telem = data.telemetry || {};
        const cr = telem.conflict_rate || 0;
        const ar = telem.abort_rate || 0;
        const compIdx = telem.composite_index || 0;
        const level = telem.contention_level || 'LOW';

        valContentionScore.textContent = compIdx.toFixed(2);
        contentionBadge.textContent = level;
        contentionBadge.className = `metric-badge level-${level.toLowerCase()}`;
        contentionBar.style.width = Math.min(100, Math.round(compIdx * 100)) + '%';
        if (level === 'HIGH') contentionBar.className = 'progress-bar-fill fill-rose';
        else if (level === 'MEDIUM') contentionBar.className = 'progress-bar-fill fill-amber';
        else contentionBar.className = 'progress-bar-fill fill-cyan';

        valAbortRate.textContent = (ar * 100).toFixed(2) + '%';
        valAbortSub.textContent = (telem.total_aborts || 0) + ' total aborts';
        abortBar.style.width = Math.min(100, Math.round(ar * 100)) + '%';

        // Predictive ML
        const forecast = data.forecasted_contention || 0;
        valForecast.textContent = forecast.toFixed(2);
        const regime = data.traffic_regime || 'NORMAL';
        regimeBadge.textContent = regime;
        if (regime === 'SALARY_CLEARING_BURST') {
            regimeBadge.className = 'metric-badge level-high';
        } else if (regime === 'UPI_EVENING_PEAK') {
            regimeBadge.className = 'metric-badge level-medium';
        } else {
            regimeBadge.className = 'metric-badge regime-badge';
        }

        const lat = data.ml_inference_latency_ns ? (data.ml_inference_latency_ns / 1000).toFixed(1) : '1.2';
        valMlLatency.textContent = `Latency: ${lat}µs`;

        const quota = data.proactive_quota || {};
        if (quota.is_compressed) {
            valQuotaStatus.textContent = `Quota: Anti-Convoy Compressed (Q=${quota.q2_batch_quantum})`;
            valQuotaStatus.className = 'text-amber';
        } else {
            valQuotaStatus.textContent = `Quota: Normal (Q=${quota.q2_batch_quantum || 5})`;
            valQuotaStatus.className = '';
        }

        // 3. CPU Core Execution Window
        const running = data.running_process;
        if (running) {
            cpuStatusTag.textContent = 'RUNNING';
            cpuStatusTag.className = 'panel-tag text-emerald';
            cpuPid.textContent = '#' + running.pid;
            cpuType.textContent = running.type;
            cpuType.className = `type-pill ${running.type}`;
            cpuAmount.textContent = '₹' + (running.amount || 0).toLocaleString(undefined, { minimumFractionDigits: 2 });
            cpuFrom.textContent = running.from_account;
            cpuTo.textContent = running.to_account;
            cpuBurst.textContent = `${running.burst_time} / ${running.total_burst_time} cycles`;

            const total = running.total_burst_time || 1;
            const remaining = running.burst_time || 0;
            const progress = Math.max(0, Math.min(100, Math.round(((total - remaining) / total) * 100)));
            cpuBurstPct.textContent = progress + '%';
            cpuBurstBar.style.width = progress + '%';

            // Locks held
            cpuLocksList.innerHTML = '';
            const locks = running.required_locks || [];
            if (locks.length === 0) {
                cpuLocksList.innerHTML = '<span class="lock-pill empty">None</span>';
            } else {
                locks.forEach(acc => {
                    const pill = document.createElement('span');
                    pill.className = 'lock-pill';
                    pill.textContent = `🔒 Acc #${acc}`;
                    cpuLocksList.appendChild(pill);
                });
            }
        } else {
            cpuStatusTag.textContent = 'IDLE';
            cpuStatusTag.className = 'panel-tag text-dim';
            cpuPid.textContent = 'IDLE';
            cpuType.textContent = '-';
            cpuType.className = 'type-pill';
            cpuAmount.textContent = '₹0.00';
            cpuFrom.textContent = '-';
            cpuTo.textContent = '-';
            cpuBurst.textContent = '0 / 0 cycles';
            cpuBurstPct.textContent = '0%';
            cpuBurstBar.style.width = '0%';
            cpuLocksList.innerHTML = '<span class="lock-pill empty">CPU Core Idle</span>';
        }

        // 4. MLFQ Pipeline Visualizer
        const q1Pids = data.queue1_pids || [];
        q1Count.textContent = `${q1Pids.length} items`;
        renderPidChips(q1Container, q1Pids, 'Q1');

        const q2Pids = data.queue2_pids || [];
        q2Count.textContent = `${q2Pids.length} items`;
        if (quota.is_compressed) {
            q2QuantumLabel.textContent = `Round Robin Batch (Salary Q=${quota.q2_batch_quantum} compressed)`;
        } else {
            q2QuantumLabel.textContent = `Round Robin Batch (Salary Q=${quota.q2_batch_quantum || 5} ticks)`;
        }
        renderPidChips(q2Container, q2Pids, 'Q2');

        const waitPids = data.waiting_pids || [];
        waitCount.textContent = `${waitPids.length} items`;
        renderPidChips(waitContainer, waitPids, 'WAIT');

        // 5. Cache Active PCBs for Table
        allPcbsCache = data.active_pcbs || [];
        renderPcbTable();
    }

    function renderPidChips(container, pids, type) {
        container.innerHTML = '';
        if (pids.length === 0) {
            container.innerHTML = `<span class="placeholder-text">${type} is empty</span>`;
            return;
        }
        const displayLimit = 25;
        pids.slice(0, displayLimit).forEach(pid => {
            const chip = document.createElement('span');
            chip.className = 'pid-chip';
            chip.textContent = `#${pid}`;
            container.appendChild(chip);
        });
        if (pids.length > displayLimit) {
            const more = document.createElement('span');
            more.className = 'placeholder-text';
            more.textContent = `+${pids.length - displayLimit} more`;
            container.appendChild(more);
        }
    }

    function renderPcbTable() {
        const query = pcbSearchInput.value.trim().toLowerCase();
        const filterState = pcbFilterStatus.value;

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
            pcbTbody.innerHTML = '<tr><td colspan="9" class="text-center placeholder-text">No active processes matching filter</td></tr>';
            return;
        }

        let html = '';
        filtered.forEach(p => {
            const qStr = p.aged_promoted ? 'Q1 (Aged)' : ('Q' + p.current_queue);
            const amtStr = '₹' + (p.amount || 0).toLocaleString(undefined, { minimumFractionDigits: 2 });
            html += `
                <tr>
                    <td><strong>#${p.pid}</strong></td>
                    <td><span class="type-pill ${p.type}">${p.type}</span></td>
                    <td><span class="status-badge status-${p.state}">${p.state}</span></td>
                    <td>${qStr}</td>
                    <td>${p.from_account} → ${p.to_account}</td>
                    <td>${amtStr}</td>
                    <td>${p.burst_time} / ${p.total_burst_time}</td>
                    <td>${p.retry_count}</td>
                    <td>${p.backoff_remaining > 0 ? (p.backoff_remaining + ' ticks') : '-'}</td>
                </tr>
            `;
        });
        pcbTbody.innerHTML = html;
    }

    async function pollTransitions() {
        try {
            const res = await fetch('/api/transitions');
            if (!res.ok) return;
            const transitions = await res.json();
            if (transitions.length === knownTransitionsCount) return;
            knownTransitionsCount = transitions.length;

            renderTransitionsFeed(transitions);
        } catch (err) {
            // Server quiet
        }
    }

    function renderTransitionsFeed(transitions) {
        transitionsFeed.innerHTML = '';
        const recent = transitions.slice(-25).reverse();
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
                    <span class="f-change">${t.from_state} → ${t.to_state}</span>
                </div>
                <div class="f-reason">${t.reason || 'State transition'}</div>
            `;
            transitionsFeed.appendChild(item);
        });
    }

    function pushTpsData(val) {
        tpsHistory.push(val);
        if (tpsHistory.length > maxTpsHistory) {
            tpsHistory.shift();
        }
    }

    function drawSparkline() {
        const w = tpsCanvas.width;
        const h = tpsCanvas.height;
        ctx.clearRect(0, 0, w, h);

        if (tpsHistory.length < 2) return;

        const maxVal = Math.max(10, Math.max(...tpsHistory));
        const step = w / (maxTpsHistory - 1);

        ctx.beginPath();
        ctx.strokeStyle = '#06b6d4';
        ctx.lineWidth = 2;
        ctx.lineJoin = 'round';

        tpsHistory.forEach((val, i) => {
            const x = i * step;
            const y = h - (val / maxVal) * (h - 6) - 3;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();

        // Subtle gradient fill under curve
        ctx.lineTo((tpsHistory.length - 1) * step, h);
        ctx.lineTo(0, h);
        ctx.closePath();
        const grad = ctx.createLinearGradient(0, 0, 0, h);
        grad.addColorStop(0, 'rgba(6, 182, 212, 0.25)');
        grad.addColorStop(1, 'rgba(6, 182, 212, 0.0)');
        ctx.fillStyle = grad;
        ctx.fill();
    }

    // Launch
    window.addEventListener('DOMContentLoaded', init);
})();
