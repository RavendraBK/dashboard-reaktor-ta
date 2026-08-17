'use strict';

require('dotenv').config();

const express = require('express');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

const requiredEnvironment = [
    'SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
    'TELEGRAM_BOT_TOKEN',
    'TELEGRAM_CHAT_IDS'
];

const missingEnvironment = requiredEnvironment.filter((key) => !process.env[key]);
if (missingEnvironment.length > 0) {
    throw new Error(`Missing required environment variable(s): ${missingEnvironment.join(', ')}`);
}

const config = Object.freeze({
    port: Number(process.env.PORT || 3000),
    mqttUrl: process.env.MQTT_URL || 'mqtts://broker.hivemq.com:8883',
    sensorTopic: process.env.MQTT_SENSOR_TOPIC || 'ta/reaktor/data_sensor',
    anomalyCooldownMs: Number(process.env.ANOMALY_COOLDOWN_MS || 60_000),
    telegramToken: process.env.TELEGRAM_BOT_TOKEN,
    telegramChatIds: process.env.TELEGRAM_CHAT_IDS.split(',').map((id) => id.trim()).filter(Boolean)
});

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY,
    { auth: { persistSession: false, autoRefreshToken: false } }
);

const stages = Object.freeze({
    1: {
        id: 1,
        name: 'Mixing',
        temp: { target: 60, tolerance: 0.10 },
        rpm: { target: 250, tolerance: 0.10 },
        durationLabel: '30 menit',
        actuators: 'Motor ON | Heater ON | Valve OFF'
    },
    2: {
        id: 2,
        name: 'Add Catalyst',
        temp: null, // Set point heater OFF; suhu cairan aktual tidak dinilai terhadap 0 °C.
        rpm: { min: 0, max: 0 },
        durationLabel: 'Momentary (timer tetap berjalan)',
        actuators: 'Motor OFF | Heater OFF | Valve katalis ON'
    },
    3: {
        id: 3,
        name: 'Reflux',
        temp: { target: 100, tolerance: 0.10 },
        rpm: { min: 0, max: 0 },
        durationLabel: '5 jam',
        actuators: 'Motor OFF | Heater ON | Valve OFF'
    },
    4: {
        id: 4,
        name: 'Separation',
        temp: null,
        rpm: { min: 0, max: 0 },
        durationLabel: '12 jam',
        actuators: 'Motor OFF | Heater OFF | Valve OFF'
    },
    5: {
        id: 5,
        name: 'Oil Treatment',
        temp: { target: 120, tolerance: 0.10 },
        rpm: { target: 250, tolerance: 0.10 },
        durationLabel: '1 jam',
        actuators: 'Motor ON | Heater ON | Valve OFF'
    },
    6: {
        id: 6,
        name: 'Filtration',
        temp: null,
        rpm: { min: 0, max: 0 },
        durationLabel: 'Estimasi 2 jam (timer tetap berjalan)',
        actuators: 'Motor OFF | Heater OFF | Valve pemisah ON',
        levelThresholdMl: 375
    }
});

const processState = {
    batchId: null,
    currentStageId: null,
    stageStartedAt: null,
    stageStats: null,
    anomalyCount: 0,
    stageAnomalyCount: 0,
    lastAlertAt: new Map(),
    levelReachedNotified: false
};

function createAutomaticBatchId() {
    return `AUTO-${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`;
}

function createEmptyStats() {
    return { count: 0, sumTemp: 0, sumRpm: 0, sumVol: 0 };
}

function getStageId(value) {
    const numericValue = Number(value);
    if (Number.isInteger(numericValue) && stages[numericValue]) return numericValue;

    const textValue = String(value ?? '').trim().toLowerCase();
    const stage = Object.values(stages).find((item) => item.name.toLowerCase() === textValue);
    return stage ? stage.id : null;
}

function toFiniteNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function isActiveSignal(value) {
    return value === true || value === 1 || ['true', '1', 'on', 'active'].includes(String(value).toLowerCase());
}

function normalisePayload(rawPayload) {
    const stageId = getStageId(rawPayload.stage ?? rawPayload.process);
    const temp = toFiniteNumber(rawPayload.temp ?? rawPayload.temperature);
    const rpm = toFiniteNumber(rawPayload.rpm);
    const vol = toFiniteNumber(rawPayload.vol ?? rawPayload.volume);

    if (!stageId || temp === null || rpm === null || vol === null) return null;

    const batchId = String(rawPayload.batch_id ?? rawPayload.batchId ?? '').trim() || null;
    return {
        stageId,
        temp,
        rpm,
        vol,
        batchId,
        levelDetected: isActiveSignal(rawPayload.level_detected ?? rawPayload.levelDetected)
    };
}

function formatNumber(value, digits = 1) {
    return Number(value).toFixed(digits);
}

function formatDuration(totalSeconds) {
    const seconds = Math.max(0, Math.floor(totalSeconds));
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainingSeconds = seconds % 60;
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(remainingSeconds).padStart(2, '0')}`;
}

function getRangeText(rule, unit) {
    if (!rule) return 'monitoring saja';
    if (Object.hasOwn(rule, 'target')) {
        const minimum = rule.target * (1 - rule.tolerance);
        const maximum = rule.target * (1 + rule.tolerance);
        return `${formatNumber(rule.target)} ${unit} (batas ${formatNumber(minimum)}–${formatNumber(maximum)} ${unit})`;
    }
    return `${rule.min}–${rule.max} ${unit}`;
}

function isOutsideRule(value, rule) {
    if (!rule) return false;
    if (Object.hasOwn(rule, 'target')) {
        return value < rule.target * (1 - rule.tolerance) || value > rule.target * (1 + rule.tolerance);
    }
    return value < rule.min || value > rule.max;
}

function getAnomalies(data, stage) {
    const anomalies = [];
    if (isOutsideRule(data.temp, stage.temp)) {
        anomalies.push({ label: 'Suhu', value: `${formatNumber(data.temp)} °C`, setPoint: getRangeText(stage.temp, '°C') });
    }
    if (isOutsideRule(data.rpm, stage.rpm)) {
        anomalies.push({ label: 'Kecepatan', value: `${Math.round(data.rpm)} RPM`, setPoint: getRangeText(stage.rpm, 'RPM') });
    }
    return anomalies;
}

async function sendTelegram(message) {
    const endpoint = `https://api.telegram.org/bot${config.telegramToken}/sendMessage`;
    await Promise.all(config.telegramChatIds.map(async (chatId) => {
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: message })
        });
        if (!response.ok) throw new Error(`Telegram API returned HTTP ${response.status}`);
    }));
}

async function notifyTelegram(message) {
    try {
        await sendTelegram(message);
        console.log('[TELEGRAM] Notification delivered.');
    } catch (error) {
        console.error('[TELEGRAM] Notification failed:', error.message);
    }
}

function buildStageStartMessage(stage) {
    return [
        '🟢 REAKTOR — PROSES DIMULAI',
        `Batch: ${processState.batchId}`,
        `Tahap ${stage.id}/6: ${stage.name}`,
        '',
        'Set point operasi:',
        `• Suhu: ${getRangeText(stage.temp, '°C')}`,
        `• Kecepatan: ${getRangeText(stage.rpm, 'RPM')}`,
        `• Volume: monitoring saja${stage.levelThresholdMl ? `; batas level akhir ≥ ${stage.levelThresholdMl} ml` : ''}`,
        `• Durasi target: ${stage.durationLabel}`,
        '',
        `Aktuator: ${stage.actuators}`,
        `Waktu mulai: ${new Date().toLocaleString('id-ID')}`
    ].join('\n');
}

function buildStageCompleteMessage(stage, stats, durationSeconds) {
    const hasData = stats.count > 0;
    const avgTemp = hasData ? `${formatNumber(stats.sumTemp / stats.count)} °C` : '-';
    const avgRpm = hasData ? `${Math.round(stats.sumRpm / stats.count)} RPM` : '-';
    const avgVol = hasData ? `${formatNumber(stats.sumVol / stats.count)} ml` : '-';

    return [
        '✅ REAKTOR — TAHAP SELESAI',
        `Batch: ${processState.batchId}`,
        `Tahap ${stage.id}/6: ${stage.name}`,
        '',
        `Durasi aktual: ${formatDuration(durationSeconds)}`,
        `Jumlah sampel: ${stats.count}`,
        'Rata-rata parameter:',
        `• Suhu: ${avgTemp}`,
        `• Kecepatan: ${avgRpm}`,
        `• Volume: ${avgVol}`,
        `• Anomali pada tahap ini: ${processState.stageAnomalyCount}`,
        `• Akumulasi anomali batch: ${processState.anomalyCount}`,
        '',
        stage.id < 6
            ? 'Tahap berikutnya akan dimulai ketika data dari ESP32 menunjukkan perubahan stage.'
            : 'Tahap filtrasi telah selesai. Pastikan batch ditutup dan laporan diunduh dari dashboard.'
    ].join('\n');
}

function buildAnomalyMessage(stage, data, anomalies) {
    const anomalyLines = anomalies.map((item) => (
        `• ${item.label}: ${item.value} | Set point: ${item.setPoint} ⚠️`
    ));

    return [
        '🚨 REAKTOR — ANOMALI TERDETEKSI',
        `Batch: ${processState.batchId}`,
        `Tahap ${stage.id}/6: ${stage.name}`,
        `Waktu: ${new Date().toLocaleString('id-ID')}`,
        '',
        'Nilai real-time:',
        `• Suhu: ${formatNumber(data.temp)} °C | Set point: ${getRangeText(stage.temp, '°C')}`,
        `• Kecepatan: ${Math.round(data.rpm)} RPM | Set point: ${getRangeText(stage.rpm, 'RPM')}`,
        `• Volume: ${formatNumber(data.vol)} ml | Status: monitoring`,
        '',
        'Parameter di luar batas:',
        ...anomalyLines,
        '',
        'Tindakan disarankan:',
        '1. Periksa sensor, heater, motor pengaduk, valve, dan koneksi kabel.',
        '2. Verifikasi kondisi reaktor secara langsung sebelum melanjutkan proses.',
        '3. Jika kondisi tidak aman, tekan tombol EMERGENCY STOP pada dashboard untuk mematikan aktuator.'
    ].join('\n');
}

function buildLevelReachedMessage(data) {
    return [
        '🧪 REAKTOR — LEVEL PRODUK AKHIR TERCAPAI',
        `Batch: ${processState.batchId}`,
        'Tahap 6/6: Filtration',
        `Sensor optik aktif. Volume terukur: ${formatNumber(data.vol)} ml.`,
        'Batas level wadah: ≥ 375 ml (75% dari beaker 500 ml).',
        'Periksa wadah penampung sebelum proses filtrasi diteruskan.'
    ].join('\n');
}

function resetForNewBatch(batchId) {
    processState.batchId = batchId;
    processState.currentStageId = null;
    processState.stageStartedAt = null;
    processState.stageStats = null;
    processState.anomalyCount = 0;
    processState.stageAnomalyCount = 0;
    processState.lastAlertAt.clear();
    processState.levelReachedNotified = false;
}

async function startStage(stageId) {
    const stage = stages[stageId];
    processState.currentStageId = stageId;
    processState.stageStartedAt = Date.now();
    processState.stageStats = createEmptyStats();
    processState.stageAnomalyCount = 0;
    processState.levelReachedNotified = false;
    await notifyTelegram(buildStageStartMessage(stage));
}

async function completeCurrentStage() {
    const stage = stages[processState.currentStageId];
    if (!stage || !processState.stageStats || !processState.stageStartedAt) return;

    const durationSeconds = (Date.now() - processState.stageStartedAt) / 1000;
    await notifyTelegram(buildStageCompleteMessage(stage, processState.stageStats, durationSeconds));
}

async function storeLog(data) {
    const { error } = await supabase.from('reactor_logs').insert([{
        batch_id: processState.batchId,
        stage: data.stageId,
        temp: data.temp,
        rpm: data.rpm,
        vol: data.vol,
        level_detected: data.levelDetected,
        recorded_at: new Date().toISOString()
    }]);
    if (error) throw new Error(error.message);
}

async function processSensorData(rawPayload) {
    const data = normalisePayload(rawPayload);
    if (!data) {
        console.warn('[MQTT] Invalid sensor payload ignored.');
        return;
    }

    const isNewExplicitBatch = data.batchId && data.batchId !== processState.batchId;
    const isImplicitRestart = !data.batchId && data.stageId === 1 && processState.currentStageId && processState.currentStageId !== 1;
    if (isNewExplicitBatch || isImplicitRestart || !processState.batchId) {
        if (processState.currentStageId) await completeCurrentStage();
        resetForNewBatch(data.batchId || createAutomaticBatchId());
    }

    if (data.stageId !== processState.currentStageId) {
        if (processState.currentStageId) await completeCurrentStage();
        await startStage(data.stageId);
    }

    processState.stageStats.count += 1;
    processState.stageStats.sumTemp += data.temp;
    processState.stageStats.sumRpm += data.rpm;
    processState.stageStats.sumVol += data.vol;

    try {
        await storeLog(data);
    } catch (error) {
        console.error('[SUPABASE] Failed to store sensor data:', error.message);
    }

    const stage = stages[data.stageId];
    const anomalies = getAnomalies(data, stage);
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
    }
}

const app = express();
app.disable('x-powered-by');
app.get('/health', (_request, response) => {
    response.status(200).json({
        status: 'ok',
        mqttConnected: client.connected,
        batchId: processState.batchId,
        currentStage: processState.currentStageId
    });
});

const client = mqtt.connect(config.mqttUrl, {
    clientId: `reactor-backend-${Math.random().toString(16).slice(2, 10)}`,
    reconnectPeriod: 5_000,
    connectTimeout: 10_000,
    clean: true
});

client.on('connect', () => {
    console.log(`[MQTT] Connected. Subscribing to ${config.sensorTopic}`);
    client.subscribe(config.sensorTopic, { qos: 1 }, (error) => {
        if (error) console.error('[MQTT] Subscription failed:', error.message);
    });
});

client.on('reconnect', () => console.warn('[MQTT] Reconnecting...'));
client.on('error', (error) => console.error('[MQTT] Error:', error.message));

let messageQueue = Promise.resolve();
client.on('message', (_topic, message) => {
    messageQueue = messageQueue
        .then(() => processSensorData(JSON.parse(message.toString())))
        .catch((error) => console.error('[MQTT] Message processing failed:', error.message));
});

app.listen(config.port, () => {
    console.log(`[HTTP] Health endpoint is ready on port ${config.port}.`);
});
