require('dotenv').config();
const path = require('node:path');
const express = require('express');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

// Sanitasi URL Supabase agar tidak ada duplikasi '/rest/v1' jika URL env menyertakannya
function cleanSupabaseUrl(url) {
    if (!url) return '';
    return url.replace(/\/rest\/v1\/?$/, '').replace(/\/+$/, '');
}

const config = {
    port: Number(process.env.PORT) || 3000,
    mqttUrl: process.env.MQTT_URL || 'mqtt://broker.hivemq.com:1883',
    sensorTopic: process.env.MQTT_SENSOR_TOPIC || 'ta/reaktor/data_sensor',
    controlTopic: process.env.MQTT_CONTROL_TOPIC || 'ta/reaktor/perintah',
    frontendOrigin: process.env.FRONTEND_ORIGIN || '*',
    telegramToken: process.env.TELEGRAM_BOT_TOKEN || '',
    supabaseUrl: cleanSupabaseUrl(process.env.SUPABASE_URL),
    supabaseServiceRole: process.env.SUPABASE_SERVICE_ROLE_KEY || '',
    supabaseAnonKey: process.env.SUPABASE_ANON_KEY || '',
    batchCodeColumn: process.env.SUPABASE_BATCH_CODE_COLUMN || 'batch_code',
    anomalyCooldownMs: 60_000
};

const supabase = createClient(config.supabaseUrl, config.supabaseServiceRole, {
    auth: { autoRefreshToken: false, persistSession: false }
});

function createUserClient(accessToken) {
    return createClient(config.supabaseUrl, config.supabaseAnonKey || config.supabaseServiceRole, {
        global: { headers: { Authorization: `Bearer ${accessToken}` } },
        auth: { autoRefreshToken: false, persistSession: false }
    });
}

const stages = {
    1: { id: 1, name: 'Mixing', durationLabel: '30 menit', targetSeconds: 1800, temp: { target: 60, tolerance: 0.10 }, rpm: { target: 250, tolerance: 0.10 }, current: { target: 0.35, min: 0, max: 1.8 }, actuators: 'Motor ON (12V 280RPM) | Heater ON (12V 2x40W) | Katup 1 Umpan Katalis' },
    2: { id: 2, name: 'Reflux', durationLabel: '5 jam', targetSeconds: 18000, temp: { target: 100, tolerance: 0.10 }, rpm: { target: 0, min: 0, max: 10 }, current: null, actuators: 'Heater ON (12V 2x40W, Motor OFF)' },
    3: { id: 3, name: 'Separation', durationLabel: '12 jam', targetSeconds: 43200, temp: null, rpm: null, current: null, actuators: 'Semua Aktuator OFF (Pemisahan Gravitasi & Air Cuci 10%)' },
    4: { id: 4, name: 'Oil Treatment', durationLabel: '1 jam', targetSeconds: 3600, temp: { target: 120, tolerance: 0.10 }, rpm: { target: 250, tolerance: 0.10 }, current: { target: 0.35, min: 0, max: 1.8 }, actuators: 'Motor ON | Heater ON | Adsorben Bentonit 0.2g' },
    5: { id: 5, name: 'Filtration', durationLabel: 'Level ≥375 ml / 2 jam', targetSeconds: 7200, temp: null, rpm: null, current: null, actuators: 'Katup 2 Pengurasan Separator ke Corong Whatman' }
};

const processState = {
    batchId: null,
    batchCode: null,
    currentStageId: null,
    stageStartedAt: null,
    batchStartedAt: null,
    stageStats: null,
    batchStats: { count: 0, sumTemp: 0, sumRpm: 0, sumVol: 0, sumCurrent: 0 },
    runStatus: 'standby',
    anomalyCount: 0,
    stageAnomalyCount: 0,
    lastAlertAt: new Map(),
    notificationChatIds: new Set(),
    latestData: null,
    levelReachedNotified: false,
    batchCompletedNotified: false,
    recipe: {
        alcoholName: 'Isopropanol',
        alcoholAmount: 40.5,
        alcoholUnit: 'ml',
        acidName: 'Palmitic Acid',
        acidAmount: 110.8,
        acidUnit: 'g',
        targetRatio: 8,
        catalystName: 'H2SO4',
        catalystVol: 6.8,
        waterVol: 17.1,
        bentoniteAmount: '0.2 g / 10 ml',
        estimatedTotalVol: 170.6,
        estimatedYieldPct: 91.5
    }
};

const sseClients = new Set();
let client = null;

function indonesiaParts(date = new Date()) {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Jakarta',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23'
    }).formatToParts(date);
    const get = (type) => parts.find((p) => p.type === type)?.value || '00';
    return {
        date: `${get('year')}-${get('month')}-${get('day')}`,
        time: `${get('hour')}:${get('minute')}:${get('second')}`,
        display: `${get('day')}/${get('month')}/${get('year')} ${get('hour')}:${get('minute')}:${get('second')} WIB`
    };
}

function createAutomaticBatchId() {
    const now = indonesiaParts();
    return `BATCH-${now.date.replaceAll('-', '')}-${now.time.replaceAll(':', '')}`;
}

function toFiniteNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function getStageId(rawStage) {
    if (typeof rawStage === 'number' && Number.isInteger(rawStage) && rawStage >= 1 && rawStage <= 5) {
        return rawStage;
    }
    const text = String(rawStage ?? '').toLowerCase().trim();
    if (/^[1-5]$/.test(text)) return Number(text);
    if (text.includes('mix')) return 1;
    if (text.includes('reflux')) return 2;
    if (text.includes('separat') || text.includes('pisah')) return 3;
    if (text.includes('oil') || text.includes('treatment') || text.includes('minyak')) return 4;
    if (text.includes('filtr') || text.includes('saring') || text.includes('done')) return 5;
    if (text.includes('standby') || text.includes('ready') || text.includes('siap')) return 1;
    return null;
}

function isActiveSignal(value) {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value === 1;
    const text = String(value ?? '').trim().toLowerCase();
    return ['1', 'true', 'on', 'active', 'aktif'].includes(text);
}

function normaliseActuators(rawActuators) {
    const source = rawActuators && typeof rawActuators === 'object' ? rawActuators : {};
    const catalystValve = isActiveSignal(source.catalyst_valve ?? source.catalystValve);
    const separatorValve = isActiveSignal(source.separator_valve ?? source.separatorValve);
    const servoValve = isActiveSignal(source.servo ?? source.servo_valve) || catalystValve || separatorValve;
    return {
        motor: isActiveSignal(source.motor ?? source.stirrer),
        heater: isActiveSignal(source.heater),
        catalyst_valve: catalystValve,
        separator_valve: separatorValve,
        servo: servoValve,
        valve: isActiveSignal(source.valve) || catalystValve || separatorValve
    };
}

function normalisePayload(rawPayload) {
    if (!rawPayload || typeof rawPayload !== 'object') return null;
    const stageId = getStageId(rawPayload.stage ?? rawPayload.stage_id ?? rawPayload.process);
    const temp = toFiniteNumber(rawPayload.temp ?? rawPayload.temperature ?? rawPayload.suhu);
    const rpm = toFiniteNumber(rawPayload.rpm ?? rawPayload.kecepatan);
    const vol = toFiniteNumber(rawPayload.vol ?? rawPayload.volume ?? rawPayload.volume_ml ?? rawPayload.massa_g);
    const act = normaliseActuators(rawPayload.act ?? rawPayload.actuators);
    const current = toFiniteNumber(rawPayload.current ?? rawPayload.current_a ?? rawPayload.ampere ?? (rawPayload.arus_ma ? (rawPayload.arus_ma / 1000) : null))
        ?? (act.motor ? 0.35 : 0);

    if (!stageId || temp === null || rpm === null || vol === null) return null;

    const batchId = String(rawPayload.batch_id ?? rawPayload.batchId ?? '').trim() || null;
    const batchCode = String(rawPayload.batch_code ?? rawPayload.batchCode ?? batchId ?? '').trim() || null;
    return {
        stageId, temp, rpm, vol, current, act,
        batchId,
        batchCode,
        levelDetected: isActiveSignal(rawPayload.level_detected ?? rawPayload.levelDetected ?? rawPayload.level_penuh)
            || (stageId === 5 && vol >= 375),
        elapsedSeconds: Math.max(0, Math.round(toFiniteNumber(rawPayload.elapsed_s ?? rawPayload.elapsed_seconds ?? rawPayload.elapsedSeconds) ?? 0)),
        rawPayload
    };
}

function createEmptyStats() {
    return { count: 0, sumTemp: 0, sumRpm: 0, sumVol: 0, sumCurrent: 0 };
}

function formatNumber(value, decimals = 1) {
    return Number(value).toFixed(decimals);
}

function formatDuration(totalSeconds) {
    const total = Math.max(0, Math.floor(totalSeconds));
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const seconds = total % 60;
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

function ruleText(rule, unit) {
    if (!rule) return 'monitoring saja';
    if (Object.hasOwn(rule, 'target') && Object.hasOwn(rule, 'tolerance')) {
        const min = rule.target * (1 - rule.tolerance);
        const max = rule.target * (1 + rule.tolerance);
        return `${formatNumber(rule.target)} ${unit} (batas ${formatNumber(min)}–${formatNumber(max)} ${unit})`;
    }
    const target = Object.hasOwn(rule, 'target') ? `set point ${formatNumber(rule.target)} ${unit}; ` : '';
    return `${target}batas ${formatNumber(rule.min)}–${formatNumber(rule.max)} ${unit}`;
}

function isOutsideRule(value, rule) {
    if (!rule) return false;
    if (Object.hasOwn(rule, 'min') || Object.hasOwn(rule, 'max')) {
        return (Object.hasOwn(rule, 'min') && value < rule.min)
            || (Object.hasOwn(rule, 'max') && value > rule.max);
    }
    return value < rule.target * (1 - rule.tolerance) || value > rule.target * (1 + rule.tolerance);
}

function getAnomalies(data, stage) {
    const candidates = [
        ['Suhu', data.temp, stage.temp, '°C', 1],
        ['Kecepatan', data.rpm, stage.rpm, 'RPM', 0],
        ['Arus motor', data.current, stage.current, 'A', 2]
    ];
    return candidates.filter(([, value, rule]) => isOutsideRule(value, rule)).map(([label, value, rule, unit, decimals]) => ({
        label,
        value: `${formatNumber(value, decimals)} ${unit}`,
        setPoint: ruleText(rule, unit)
    }));
}

function resetForNewBatch(batchId, batchCode = batchId) {
    processState.batchId = batchId;
    processState.batchCode = batchCode || batchId;
    processState.currentStageId = null;
    processState.stageStartedAt = null;
    processState.batchStartedAt = Date.now();
    processState.stageStats = null;
    processState.batchStats = createEmptyStats();
    processState.anomalyCount = 0;
    processState.stageAnomalyCount = 0;
    processState.lastAlertAt.clear();
    processState.levelReachedNotified = false;
    processState.batchCompletedNotified = false;
}

function getPublicState() {
    return {
        batchId: processState.batchId,
        batchCode: processState.batchCode,
        currentStageId: processState.currentStageId,
        currentStageName: stages[processState.currentStageId]?.name || '-',
        runStatus: processState.runStatus,
        anomalyCount: processState.anomalyCount,
        latestData: processState.latestData,
        recipe: processState.recipe,
        updatedAt: new Date().toISOString()
    };
}

function broadcast(event, payload) {
    const message = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const response of sseClients) response.write(message);
}

function broadcastState() {
    broadcast('state', getPublicState());
}

async function sendTelegram(message) {
    const recipients = [...processState.notificationChatIds];
    await Promise.all(recipients.map(async (chatId) => {
        const response = await fetch(`https://api.telegram.org/bot${config.telegramToken}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: message })
        });
        if (!response.ok) throw new Error(`Telegram API menghasilkan HTTP ${response.status}`);
    }));
}

async function notifyTelegram(message) {
    try {
        await sendTelegram(message);
        console.log('[TELEGRAM] Notifikasi terkirim.');
    } catch (error) {
        console.error('[TELEGRAM] Gagal mengirim:', error.message);
    }
}

function buildStageStartMessage(stage) {
    return [
        '🟢 REAKTOR — TAHAP DIMULAI',
        `Batch: ${processState.batchCode || processState.batchId}`,
        `Tahap ${stage.id}/6: ${stage.name}`,
        '',
        `• Suhu: ${ruleText(stage.temp, '°C')}`,
        `• RPM: ${ruleText(stage.rpm, 'RPM')}`,
        `• Arus motor: ${ruleText(stage.current, 'A')}`,
        `• Durasi target: ${stage.durationLabel}`,
        `• Aktuator: ${stage.actuators}`,
        `Waktu mulai: ${indonesiaParts().display}`
    ].join('\n');
}

function buildStageCompleteMessage(stage, stats, durationSeconds) {
    const divisor = stats.count || 1;
    return [
        '✅ REAKTOR — TAHAP SELESAI',
        `Batch: ${processState.batchCode || processState.batchId}`,
        `Tahap ${stage.id}/6: ${stage.name}`,
        `Durasi aktual: ${formatDuration(durationSeconds)}`,
        `Jumlah sampel: ${stats.count}`,
        `Rata-rata suhu: ${formatNumber(stats.sumTemp / divisor)} °C`,
        `Rata-rata RPM: ${Math.round(stats.sumRpm / divisor)} RPM`,
        `Rata-rata volume: ${formatNumber(stats.sumVol / divisor)} ml`,
        `Rata-rata arus: ${formatNumber(stats.sumCurrent / divisor, 2)} A`,
        `Anomali tahap/batch: ${processState.stageAnomalyCount}/${processState.anomalyCount}`
    ].join('\n');
}

function buildBatchSummaryMessage(reason = 'Tahap 6 / Filtrasi selesai') {
    const bStats = processState.batchStats || createEmptyStats();
    const divisor = bStats.count || 1;
    const recipe = processState.recipe || {};
    const totalDurationSeconds = processState.batchStartedAt ? Math.round((Date.now() - processState.batchStartedAt) / 1000) : 0;
    
    const avgCurrent = bStats.sumCurrent / divisor;
    const motorWatts = 12 * avgCurrent;
    const heaterAvgWatts = 45.0; // DC 12V 2x40W avg
    const servoWatts = 4.2; // DC 6V 0.7A
    const totalWatts = motorWatts + heaterAvgWatts + 1.0;
    const totalKWh = (totalWatts * (totalDurationSeconds / 3600)) / 1000;

    const finalVol = processState.latestData?.vol || 0;
    const initialVol = 500;
    const yieldPct = Math.min(100, Math.max(0, (finalVol / initialVol) * 100));

    return [
        '📋 REAKTOR — RINGKASAN AKHIR BATCH',
        `Batch Code: ${processState.batchCode || processState.batchId}`,
        `Waktu Selesai: ${indonesiaParts().display}`,
        `Status: ${reason}`,
        `Total Durasi: ${formatDuration(totalDurationSeconds)} (${bStats.count} sampel)`,
        '',
        '🧪 FORMULASI REAKTAN & KATALIS:',
        `• Alkohol: ${recipe.alcoholName || 'Isopropanol'} (${recipe.alcoholAmount || 40.5} ${recipe.alcoholUnit || 'ml'})`,
        `• Asam Karboksilat: ${recipe.acidName || 'Palmitic Acid'} (${recipe.acidAmount || 110.8} ${recipe.acidUnit || 'g'})`,
        `• Rasio Mol: 1 (Alkohol) : ${recipe.targetRatio || 8} (Asam)`,
        `• Katalis: ${recipe.catalystName || 'H2SO4'} (4% Vol ~${recipe.catalystVol || 6.8} ml)`,
        `• Air Pencucian: 10% Vol Larutan (~${recipe.waterVol || 17.1} ml)`,
        `• Adsorben Bentonit: 0.2 g (~10 ml)`,
        '',
        '📊 HASIL & PERFORMA PRODUKSI:',
        `• Volume Produk Akhir: ${formatNumber(finalVol)} ml (Yield: ${formatNumber(yieldPct)}%)`,
        `• Rata-rata Suhu: ${formatNumber(bStats.sumTemp / divisor)} °C`,
        `• Rata-rata RPM: ${Math.round(bStats.sumRpm / divisor)} RPM`,
        `• Daya Motor DC 12V 280RPM: ${formatNumber(motorWatts, 1)} W (${formatNumber(avgCurrent, 2)} A)`,
        `• Daya Heater DC 12V 2x40W (Max 80W): ~${formatNumber(heaterAvgWatts, 1)} W (~3.75 A)`,
        `• Daya Servo DC 6V 0.7A Valve: ~${formatNumber(servoWatts, 1)} W (Saat Valve ON)`,
        `• Total Energi Terpakai: ${formatNumber(totalKWh, 4)} kWh (${formatNumber(totalKWh * 1000, 1)} Wh)`,
        `• Total Anomali: ${processState.anomalyCount} kali`,
        '',
        'Data batch lengkap telah tersimpan di Supabase dan dapat diunduh dalam format PDF/CSV melalui dashboard.'
    ].join('\n');
}

function buildAnomalyMessage(stage, data, anomalies) {
    return [
        '🚨 REAKTOR — ANOMALI TERDETEKSI',
        `Batch: ${processState.batchCode || processState.batchId}`,
        `Tahap ${stage.id}/6: ${stage.name}`,
        `Waktu: ${indonesiaParts().display}`,
        '',
        `• Suhu: ${formatNumber(data.temp)} °C`,
        `• RPM: ${Math.round(data.rpm)} RPM`,
        `• Volume: ${formatNumber(data.vol)} ml`,
        `• Arus motor: ${formatNumber(data.current, 2)} A`,
        '',
        'Parameter di luar batas:',
        ...anomalies.map((item) => `• ${item.label}: ${item.value} | ${item.setPoint}`),
        '',
        'Periksa alat secara langsung. Jika tidak aman, tekan EMERGENCY STOP pada dashboard.'
    ].join('\n');
}

function buildLevelReachedMessage(data) {
    return [
        '🧪 REAKTOR — LEVEL PRODUK AKHIR TERCAPAI',
        `Batch: ${processState.batchCode || processState.batchId}`,
        'Tahap 6/6: Filtration',
        `Sensor level aktif; volume terukur ${formatNumber(data.vol)} ml.`,
        'Batas wadah akhir: ≥375 ml (75% beaker 500 ml).'
    ].join('\n');
}

async function startStage(stageId) {
    processState.currentStageId = stageId;
    processState.stageStartedAt = Date.now();
    processState.stageStats = createEmptyStats();
    processState.stageAnomalyCount = 0;
    processState.levelReachedNotified = false;
    await notifyTelegram(buildStageStartMessage(stages[stageId]));
}

async function completeCurrentStage() {
    const stage = stages[processState.currentStageId];
    if (!stage || !processState.stageStats || !processState.stageStartedAt) return;
    await notifyTelegram(buildStageCompleteMessage(
        stage,
        processState.stageStats,
        (Date.now() - processState.stageStartedAt) / 1000
    ));
}

function toDatabaseRow(data, event = null) {
    const now = indonesiaParts();
    const recordedAt = new Date().toISOString();
    const row = {
        stage: data.stageId,
        temp: data.temp,
        rpm: Math.round(data.rpm),
        vol: data.vol,
        current: data.current,
        waktu_lokal: now.time,
        tanggal: now.date,
        created_at: recordedAt,
        batch_id: processState.batchId || data.batchId || createAutomaticBatchId(),
        level_detected: Boolean(data.levelDetected),
        recorded_at: recordedAt,
        sensor_payload: {
            ...data.rawPayload,
            act: data.act,
            event,
            recipe: processState.recipe,
            received_at: recordedAt,
            backend_run_status: processState.runStatus
        }
    };
    row[config.batchCodeColumn] = processState.batchCode || data.batchCode || row.batch_id;
    return row;
}

async function storeLog(data, event = null) {
    const { error } = await supabase.from('reactor_logs').insert([toDatabaseRow(data, event)]);
    if (error) throw new Error(error.message);
}

async function storeControlEvent(command) {
    if (!processState.batchId) return;
    const latest = processState.latestData || {
        stageId: processState.currentStageId || 1,
        temp: 0, rpm: 0, vol: 0, current: 0, levelDetected: false,
        act: {}, rawPayload: {}
    };
    try {
        await storeLog(latest, { type: 'control', command, at: new Date().toISOString() });
    } catch (error) {
        console.error('[SUPABASE] Gagal merekam perintah kontrol:', error.message);
    }
}

async function processSensorData(rawPayload) {
    const data = normalisePayload(rawPayload);
    if (!data) {
        console.warn('[MQTT] Payload sensor tidak lengkap, diabaikan.');
        return;
    }

    const implicitRestart = !data.batchId && data.stageId === 1
        && processState.currentStageId && processState.currentStageId !== 1;
    const newExplicitBatch = data.batchId && data.batchId !== processState.batchId;
    if (!processState.batchId || implicitRestart || newExplicitBatch) {
        if (processState.currentStageId) await completeCurrentStage();
        resetForNewBatch(data.batchId || createAutomaticBatchId(), data.batchCode || data.batchId);
    }

    if (data.stageId !== processState.currentStageId) {
        if (processState.currentStageId) await completeCurrentStage();
        await startStage(data.stageId);
    }

    let incomingStatus = String(rawPayload.status ?? rawPayload.machine_status ?? rawPayload.run_status ?? '').toLowerCase();
    if (!incomingStatus && typeof rawPayload.running === 'boolean') {
        incomingStatus = rawPayload.running ? 'running' : 'paused';
    }
    if (['standby', 'running', 'paused', 'emergency'].includes(incomingStatus)) {
        processState.runStatus = incomingStatus;
    } else if (!processState.runStatus || processState.runStatus === 'standby') {
        processState.runStatus = 'running';
    }
    processState.latestData = data;
    
    processState.stageStats.count += 1;
    processState.stageStats.sumTemp += data.temp;
    processState.stageStats.sumRpm += data.rpm;
    processState.stageStats.sumVol += data.vol;
    processState.stageStats.sumCurrent += data.current;

    processState.batchStats.count += 1;
    processState.batchStats.sumTemp += data.temp;
    processState.batchStats.sumRpm += data.rpm;
    processState.batchStats.sumVol += data.vol;
    processState.batchStats.sumCurrent += data.current;

    try {
        await storeLog(data);
    } catch (error) {
        console.error('[SUPABASE] Gagal menyimpan data sensor:', error.message);
    }

    const stage = stages[data.stageId];
    const anomalies = processState.runStatus === 'running' ? getAnomalies(data, stage) : [];
    broadcastState();
    if (anomalies.length) {
        broadcast('anomaly', { stageId: stage.id, stageName: stage.name, anomalies, data });
    }

    for (const anomaly of anomalies) {
        const alertKey = `${processState.batchId}:${stage.id}:${anomaly.label}`;
        const previousAlert = processState.lastAlertAt.get(alertKey) || 0;
        if (Date.now() - previousAlert >= config.anomalyCooldownMs) {
            processState.anomalyCount += 1;
            processState.stageAnomalyCount += 1;
            processState.lastAlertAt.set(alertKey, Date.now());
            await notifyTelegram(buildAnomalyMessage(stage, data, [anomaly]));
        }
    }

    if (stage.id === 6 && data.levelDetected && !processState.levelReachedNotified) {
        processState.levelReachedNotified = true;
        await notifyTelegram(buildLevelReachedMessage(data));
        
        if (!processState.batchCompletedNotified) {
            processState.batchCompletedNotified = true;
            await notifyTelegram(buildBatchSummaryMessage('Filtrasi Selesai (Level ≥375 ml Tercapai)'));
        }
    }
}

function getBearerToken(request) {
    const header = request.get('authorization') || '';
    return header.startsWith('Bearer ') ? header.slice(7) : String(request.query.token || '');
}

async function requireUser(request, response, next) {
    const token = getBearerToken(request);
    if (!token) return response.status(401).json({ error: 'Silakan login terlebih dahulu.' });
    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data.user) return response.status(401).json({ error: 'Sesi login tidak valid atau telah berakhir.' });
    request.user = data.user;
    return next();
}

function publishControl(command) {
    if (!client?.connected) throw new Error('MQTT belum terhubung; perintah tidak dikirim.');
    
    let espCmd = {};
    const stageMap = { 1: 'mixing', 2: 'reflux', 3: 'separation', 4: 'oil_treatment', 5: 'filtration' };

    if (command.action === 'start') {
        if (command.start_stage && Number(command.start_stage) > 1) {
            espCmd = { cmd: 'goto', stage: stageMap[command.start_stage] || 'mixing' };
        } else {
            espCmd = { cmd: 'start' };
        }
    } else if (command.action === 'pause') {
        espCmd = { cmd: 'pause' };
    } else if (command.action === 'resume') {
        espCmd = { cmd: 'resume' };
    } else if (command.action === 'restart') {
        espCmd = { cmd: 'stop' };
    } else if (command.action === 'emergency_stop') {
        espCmd = { cmd: 'stop' };
    } else if (command.action === 'reset_emergency') {
        espCmd = { cmd: 'reset' };
    } else if (command.action === 'tare') {
        espCmd = { cmd: 'tare' };
    } else if (command.action === 'next') {
        espCmd = { cmd: 'next' };
    } else {
        espCmd = { cmd: command.action, ...command };
    }

    client.publish(config.controlTopic, JSON.stringify(espCmd), { qos: 1, retain: false });
    console.log(`[MQTT] Control dikirim ke ESP32 (${config.controlTopic}):`, espCmd);
}

function applyLocalControl(command) {
    switch (command.action) {
        case 'start':
        case 'resume':
        case 'restart':
            processState.runStatus = 'running';
            break;
        case 'pause':
            processState.runStatus = 'paused';
            break;
        case 'emergency_stop':
            processState.runStatus = 'emergency';
            break;
        default:
            break;
    }
    if (command.action === 'start' || command.action === 'restart') {
        resetForNewBatch(command.batch_id, command.batch_code);
        if (command.start_stage && stages[command.start_stage]) {
            processState.currentStageId = Number(command.start_stage);
            processState.stageStartedAt = Date.now();
            processState.stageStats = createEmptyStats();
        }
    }
    if (command.action === 'set_actuator' && processState.latestData) {
        processState.latestData.act[command.actuator] = Boolean(command.enabled);
    }
}

const app = express();
app.disable('x-powered-by');
app.use((request, response, next) => {
    const origin = request.get('origin');
    if (origin) {
        const allowed = (config.frontendOrigin || '')
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean);
        const isLocalhost = /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin);
        if (allowed.length === 0 || allowed.includes('*') || allowed.includes(origin) || isLocalhost) {
            response.set('Access-Control-Allow-Origin', origin);
            response.set('Vary', 'Origin');
        }
    }
    response.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    response.set('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    if (request.method === 'OPTIONS') return response.sendStatus(204);
    return next();
});
app.use(express.json({ limit: '100kb' }));
app.use('/assets', express.static(path.join(__dirname, 'assets')));

app.get('/health', (_request, response) => {
    response.json({ status: 'ok', mqttConnected: Boolean(client?.connected), ...getPublicState() });
});

// Endpoint untuk merekam pengunjung pameran / guest access log
app.post('/api/visitors', async (request, response) => {
    const { visitorName, enteredAt, exitedAt } = request.body || {};
    if (!visitorName) return response.status(400).json({ error: 'Nama pengunjung wajib diisi.' });

    const now = indonesiaParts();
    const visitorRow = {
        stage: 1,
        temp: 0, rpm: 0, vol: 0, current: 0,
        waktu_lokal: now.time,
        tanggal: now.date,
        created_at: new Date().toISOString(),
        batch_id: 'EXHIBITION-GUEST-ACCESS',
        level_detected: false,
        recorded_at: new Date().toISOString(),
        sensor_payload: {
            event_type: 'exhibition_guest_visit',
            visitor_name: String(visitorName).trim(),
            entered_at: enteredAt || new Date().toISOString(),
            exited_at: exitedAt || null,
            recorded_display: now.display
        }
    };
    visitorRow[config.batchCodeColumn] = 'EXHIBITION-GUEST-ACCESS';

    try {
        await supabase.from('reactor_logs').insert([visitorRow]);
        console.log(`[EXHIBITION] Pengunjung tercatat: ${visitorName} pada ${now.display}`);
        return response.json({ ok: true, visitorName, loggedAt: now.display });
    } catch (err) {
        console.warn(`[EXHIBITION] Warning: Gagal menyimpan log tamu: ${err.message}`);
        return response.json({ ok: true, visitorName, warning: err.message });
    }
});

app.post('/api/auth/login', async (request, response) => {
    const { email, password, telegramId } = request.body || {};
    if (!email || !password || !telegramId) {
        return response.status(400).json({ error: 'Email, password, dan Telegram Chat ID wajib diisi.' });
    }
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    if (error || !data.session || !data.user) {
        return response.status(401).json({ error: error?.message || 'Email atau password tidak sesuai.' });
    }

    const telegramChatId = String(telegramId).trim();
    const userClient = createUserClient(data.session.access_token);

    let profile = null;
    let { data: pData, error: profileError } = await supabase
        .from('profiles').select('telegram_chat_id').eq('id', data.user.id).maybeSingle();
    if (profileError) {
        const userCheck = await userClient
            .from('profiles').select('telegram_chat_id').eq('id', data.user.id).maybeSingle();
        if (!userCheck.error) {
            pData = userCheck.data;
            profileError = null;
        }
    }
    if (pData) profile = pData;

    if (profile?.telegram_chat_id && profile.telegram_chat_id !== telegramChatId) {
        return response.status(403).json({ error: 'Telegram Chat ID tidak sesuai dengan profil akun ini.' });
    }

    const profilePayload = {
        id: data.user.id,
        telegram_chat_id: telegramChatId,
        updated_at: new Date().toISOString()
    };

    let { error: upsertError } = await supabase.from('profiles').upsert(profilePayload, { onConflict: 'id' });
    if (upsertError) {
        console.warn(`[AUTH] Admin upsert profil gagal (${upsertError.message}), mencoba fallback dengan token user...`);
        const userUpsert = await userClient.from('profiles').upsert(profilePayload, { onConflict: 'id' });
        upsertError = userUpsert.error;
    }

    processState.notificationChatIds.add(telegramChatId);
    return response.json({
        accessToken: data.session.access_token,
        user: { email: data.user.email, telegramChatId }
    });
});

app.get('/api/state', requireUser, (_request, response) => response.json(getPublicState()));

app.post('/api/recipe', requireUser, (request, response) => {
    const newRecipe = request.body || {};
    processState.recipe = { ...processState.recipe, ...newRecipe };
    broadcastState();
    return response.json({ ok: true, recipe: processState.recipe });
});

app.post('/api/summary', requireUser, async (request, response) => {
    const reason = request.body?.reason || 'Permintaan manual operator dari dashboard';
    const message = buildBatchSummaryMessage(reason);
    await notifyTelegram(message);
    return response.json({ ok: true, message: 'Laporan ringkasan batch telah dikirim ke Telegram.' });
});

app.get('/api/events', requireUser, (request, response) => {
    response.status(200).set({
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no'
    });
    response.flushHeaders();
    response.write(`event: state\ndata: ${JSON.stringify(getPublicState())}\n\n`);
    sseClients.add(response);
    const keepAlive = setInterval(() => response.write(': keep-alive\n\n'), 25_000);
    request.on('close', () => {
        clearInterval(keepAlive);
        sseClients.delete(response);
    });
});

app.get('/api/logs', requireUser, async (request, response) => {
    const limit = Math.min(Math.max(Number(request.query.limit) || 500, 1), 2_000);
    let query = supabase.from('reactor_logs').select('*').order('recorded_at', { ascending: false }).limit(limit);
    if (request.query.batchId) query = query.eq('batch_id', String(request.query.batchId));
    const { data, error } = await query;
    if (error) return response.status(500).json({ error: error.message });
    return response.json((data || []).reverse().map((row) => ({
        ...row,
        batch_code: row[config.batchCodeColumn] ?? row.batch_code ?? row['batch code'] ?? row.batch_id
    })));
});

app.get('/api/batches', requireUser, async (_request, response) => {
    const { data, error } = await supabase.from('reactor_logs')
        .select('batch_id, recorded_at, batch_code')
        .order('recorded_at', { ascending: false }).limit(1_000);
    if (error) return response.status(500).json({ error: error.message });
    const unique = new Map();
    for (const row of data || []) {
        if (!unique.has(row.batch_id)) unique.set(row.batch_id, {
            batchId: row.batch_id,
            batchCode: row[config.batchCodeColumn] ?? row.batch_code ?? row['batch code'] ?? row.batch_id,
            latestAt: row.recorded_at
        });
    }
    return response.json([...unique.values()]);
});

app.delete('/api/batches/:batchId', requireUser, async (request, response) => {
    const batchId = String(request.params.batchId || '').trim();
    if (!batchId) return response.status(400).json({ error: 'Batch ID wajib disertakan.' });

    const { error } = await supabase.from('reactor_logs').delete().eq('batch_id', batchId);
    if (error) return response.status(500).json({ error: error.message });

    if (processState.batchId === batchId) {
        resetForNewBatch(null, null);
        processState.runStatus = 'standby';
        broadcastState();
    }
    return response.json({ ok: true, deletedBatchId: batchId, message: `Data log batch ${batchId} berhasil dihapus permanen.` });
});

app.post('/api/control', requireUser, async (request, response) => {
    const body = request.body || {};
    const allowedActions = new Set(['start', 'pause', 'resume', 'restart', 'emergency_stop', 'reset_emergency', 'set_actuator', 'complete_batch']);
    if (!allowedActions.has(body.action)) return response.status(400).json({ error: 'Aksi kontrol tidak dikenal.' });
    if (body.action === 'set_actuator' && !['motor', 'heater', 'catalyst_valve', 'separator_valve', 'servo'].includes(body.actuator)) {
        return response.status(400).json({ error: 'Aktuator tidak dikenal.' });
    }

    const batchCode = String(body.batch_code || processState.batchCode || '').trim();
    const batchId = String(body.batch_id || processState.batchId || batchCode || createAutomaticBatchId()).trim();

    if ((body.action === 'start' || body.action === 'restart') && !batchCode) {
        return response.status(400).json({ error: 'Batch Code wajib diisi sebelum memulai proses.' });
    }
    
    if (body.action === 'complete_batch') {
        await notifyTelegram(buildBatchSummaryMessage('Dihentikan / Selesai Manual oleh Operator'));
        processState.runStatus = 'standby';
        broadcastState();
        return response.json({ ok: true, state: getPublicState() });
    }

    const command = {
        action: body.action,
        batch_id: batchId,
        batch_code: batchCode || batchId,
        start_stage: body.start_stage ? Number(body.start_stage) : 1,
        actuator: body.actuator,
        enabled: Boolean(body.enabled),
        requested_by: request.user.email,
        requested_at: new Date().toISOString()
    };
    try {
        publishControl(command);
    } catch (error) {
        return response.status(503).json({ error: error.message });
    }
    applyLocalControl(command);
    broadcastState();
    await storeControlEvent(command);
    return response.json({ ok: true, command, state: getPublicState() });
});

app.get('/', (_request, response) => response.sendFile(path.join(__dirname, 'index.html')));
app.use((error, _request, response, _next) => {
    console.error('[HTTP] Error:', error.message);
    response.status(400).json({ error: 'Request tidak dapat diproses.' });
});

client = mqtt.connect(config.mqttUrl, {
    clientId: `reactor-backend-${Math.random().toString(16).slice(2, 10)}`,
    reconnectPeriod: 5_000,
    connectTimeout: 10_000,
    clean: true
});

client.on('connect', () => {
    console.log(`[MQTT] Terhubung ke ${config.mqttUrl}`);
    const topics = [config.sensorTopic, 'ta/reaktor/detail', 'ta/reaktor/log', 'ta/reaktor/status'];
    client.subscribe(topics, { qos: 1 }, (error) => {
        if (error) console.error('[MQTT] Subscribe gagal:', error.message);
        else console.log('[MQTT] Berhasil subscribe ke topik telemetri ESP32:', topics);
    });
});
client.on('reconnect', () => console.warn('[MQTT] Menghubungkan ulang...'));
client.on('error', (error) => console.error('[MQTT] Error:', error.message));

let messageQueue = Promise.resolve();
client.on('message', (topic, message) => {
    if (topic === config.sensorTopic) {
        try {
            const payload = JSON.parse(message.toString());
            messageQueue = messageQueue.then(() => processSensorData(payload))
                .catch((error) => console.error('[MQTT] Pesan sensor gagal diproses:', error.message));
        } catch (error) {
            console.warn('[MQTT] Parsing payload sensor gagal:', error.message);
        }
    } else if (topic === 'ta/reaktor/status') {
        console.log('[MQTT] Status ESP32:', message.toString());
    } else if (topic === 'ta/reaktor/log') {
        try {
            const logEntry = JSON.parse(message.toString());
            console.log(`[ESP32 LOG] [${logEntry.ev || 'EV'}] ${logEntry.stage || ''} -> ${logEntry.det || ''}`);
        } catch (_) {}
    }
});

const keepAliveUrl = process.env.KEEP_ALIVE_URL
    || process.env.RENDER_EXTERNAL_URL
    || (process.env.NODE_ENV === 'production' ? 'https://dashboard-reaktor-ta.onrender.com' : '');
const keepAliveIntervalMs = Math.max(60_000, Number(process.env.KEEP_ALIVE_INTERVAL_MS || 10 * 60 * 1000));

if (keepAliveUrl && keepAliveUrl.startsWith('http')) {
    const target = `${keepAliveUrl.replace(/\/+$/, '')}/health`;
    console.log(`[KEEP-ALIVE] Auto-pinger aktif menuju ${target} setiap ${Math.round(keepAliveIntervalMs / 60_000)} menit.`);
    setInterval(async () => {
        try {
            const res = await fetch(target, { signal: AbortSignal.timeout(15_000) });
            if (res.ok) {
                console.log(`[KEEP-ALIVE] Ping sukses ke ${target} pada ${indonesiaParts().display}`);
            }
        } catch (err) {
            console.warn(`[KEEP-ALIVE] Ping warning: ${err.message}`);
        }
    }, keepAliveIntervalMs);
}

app.listen(config.port, '0.0.0.0', () => {
    console.log(`[HTTP] Dashboard/API siap di http://localhost:${config.port}`);
});
