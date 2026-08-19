'use strict';

require('dotenv').config();

const path = require('node:path');
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
if (missingEnvironment.length) {
    throw new Error(`Variabel .env belum diisi: ${missingEnvironment.join(', ')}`);
}

const config = Object.freeze({
    port: Number(process.env.PORT || 3000),
    frontendOrigin: process.env.FRONTEND_ORIGIN || '',
    mqttUrl: process.env.MQTT_URL || 'mqtt://broker.hivemq.com:1883',
    sensorTopic: process.env.MQTT_SENSOR_TOPIC || 'ta/reaktor/data_sensor',
    controlTopic: process.env.MQTT_CONTROL_TOPIC || 'ta/reaktor/control',
    anomalyCooldownMs: Number(process.env.ANOMALY_COOLDOWN_MS || 60_000),
    telegramToken: process.env.TELEGRAM_BOT_TOKEN,
    telegramChatIds: process.env.TELEGRAM_CHAT_IDS.split(',').map((id) => id.trim()).filter(Boolean),
    // Gunakan "batch code" hanya bila kolom Supabase memang terlanjur memakai spasi.
    batchCodeColumn: process.env.REACTOR_BATCH_CODE_COLUMN || 'batch_code'
});

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false }
});

// Nilai proses yang dinilai. Temp null = sensor tetap dicatat, tetapi tidak dibandingkan
// dengan 0 °C karena heater memang dimatikan pada tahap tersebut.
const stages = Object.freeze({
    1: {
        id: 1, name: 'Mixing', durationLabel: '30 menit',
        temp: { target: 60, tolerance: 0.10 }, rpm: { target: 250, tolerance: 0.10 },
        current: { target: 2, min: 0, max: 3.2 },
        actuators: 'Motor ON | Heater ON | Valve katalis OFF | Valve pemisah OFF'
    },
    2: {
        id: 2, name: 'Add Catalyst', durationLabel: 'Momentary (timer batch tetap berjalan)',
        temp: null, rpm: { min: 0, max: 0 }, current: { min: 0, max: 0 },
        actuators: 'Motor OFF | Heater OFF | Valve katalis ON | Valve pemisah OFF'
    },
    3: {
        id: 3, name: 'Reflux', durationLabel: '5 jam',
        temp: { target: 100, tolerance: 0.10 }, rpm: { min: 0, max: 0 }, current: { min: 0, max: 0 },
        actuators: 'Motor OFF | Heater ON | Valve katalis OFF | Valve pemisah OFF'
    },
    4: {
        id: 4, name: 'Separation', durationLabel: '12 jam',
        temp: null, rpm: { min: 0, max: 0 }, current: { min: 0, max: 0 },
        actuators: 'Motor OFF | Heater OFF | Semua valve OFF'
    },
    5: {
        id: 5, name: 'Oil Treatment', durationLabel: '1 jam',
        temp: { target: 120, tolerance: 0.10 }, rpm: { target: 250, tolerance: 0.10 },
        current: { target: 2, min: 0, max: 3.2 },
        actuators: 'Motor ON | Heater ON | Semua valve OFF'
    },
    6: {
        id: 6, name: 'Filtration', durationLabel: 'Estimasi 2 jam (timer batch tetap berjalan)',
        temp: null, rpm: { min: 0, max: 0 }, current: { min: 0, max: 0 }, levelThresholdMl: 375,
        actuators: 'Motor OFF | Heater OFF | Valve pemisah ON'
    }
});

const processState = {
    batchId: null,
    batchCode: null,
    currentStageId: null,
    stageStartedAt: null,
    stageStats: null,
    anomalyCount: 0,
    stageAnomalyCount: 0,
    lastAlertAt: new Map(),
    levelReachedNotified: false,
    latestData: null,
    runStatus: 'standby', // standby | running | paused | emergency
    notificationChatIds: new Set(config.telegramChatIds)
};

const sseClients = new Set();
let client;

function indonesiaParts(date = new Date()) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Jakarta', year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'
    });
    const parts = Object.fromEntries(formatter.formatToParts(date)
        .filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
    return {
        date: `${parts.year}-${parts.month}-${parts.day}`,
        time: `${parts.hour}:${parts.minute}:${parts.second}`,
        display: `${parts.day}/${parts.month}/${parts.year} ${parts.hour}:${parts.minute}:${parts.second} WIB`
    };
}

function createAutomaticBatchId() {
    const now = indonesiaParts();
    return `BATCH-${now.date.replaceAll('-', '')}-${now.time.replaceAll(':', '')}`;
}

function getStageId(value) {
    const numeric = Number(value);
    if (Number.isInteger(numeric) && stages[numeric]) return numeric;
    const label = String(value ?? '').trim().toLowerCase();
    return Object.values(stages).find((stage) => stage.name.toLowerCase() === label)?.id || null;
}

function toFiniteNumber(value) {
    const result = Number(value);
    return Number.isFinite(result) ? result : null;
}

function isActiveSignal(value) {
    return value === true || value === 1 || ['true', '1', 'on', 'active'].includes(String(value).toLowerCase());
}

function normaliseActuators(rawAct = {}) {
    const source = rawAct && typeof rawAct === 'object' ? rawAct : {};
    const catalystValve = isActiveSignal(source.catalyst_valve ?? source.catalystValve);
    const separatorValve = isActiveSignal(source.separator_valve ?? source.separatorValve);
    return {
        motor: isActiveSignal(source.motor),
        heater: isActiveSignal(source.heater),
        catalyst_valve: catalystValve,
        separator_valve: separatorValve,
        // valve dipertahankan agar payload ESP32 versi lama tetap terbaca.
        valve: isActiveSignal(source.valve) || catalystValve || separatorValve
    };
}

function normalisePayload(rawPayload) {
    if (!rawPayload || typeof rawPayload !== 'object') return null;
    const stageId = getStageId(rawPayload.stage ?? rawPayload.process);
    const temp = toFiniteNumber(rawPayload.temp ?? rawPayload.temperature);
    const rpm = toFiniteNumber(rawPayload.rpm);
    const vol = toFiniteNumber(rawPayload.vol ?? rawPayload.volume);
    const act = normaliseActuators(rawPayload.act ?? rawPayload.actuators);
    const current = toFiniteNumber(rawPayload.current ?? rawPayload.current_a ?? rawPayload.ampere)
        ?? (act.motor ? 2 : 0);

    if (!stageId || temp === null || rpm === null || vol === null) return null;

    const batchId = String(rawPayload.batch_id ?? rawPayload.batchId ?? '').trim() || null;
    const batchCode = String(rawPayload.batch_code ?? rawPayload.batchCode ?? batchId ?? '').trim() || null;
    return {
        stageId, temp, rpm, vol, current, act,
        batchId,
        batchCode,
        levelDetected: isActiveSignal(rawPayload.level_detected ?? rawPayload.levelDetected)
            || (stageId === 6 && vol >= 375),
        elapsedSeconds: Math.max(0, Math.round(toFiniteNumber(rawPayload.elapsed_seconds ?? rawPayload.elapsedSeconds) ?? 0)),
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
    processState.stageStats = null;
    processState.anomalyCount = 0;
    processState.stageAnomalyCount = 0;
    processState.lastAlertAt.clear();
    processState.levelReachedNotified = false;
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

    processState.runStatus = String(rawPayload.machine_status ?? rawPayload.run_status ?? processState.runStatus).toLowerCase();
    if (!['standby', 'running', 'paused', 'emergency'].includes(processState.runStatus)) processState.runStatus = 'running';
    processState.latestData = data;
    processState.stageStats.count += 1;
    processState.stageStats.sumTemp += data.temp;
    processState.stageStats.sumRpm += data.rpm;
    processState.stageStats.sumVol += data.vol;
    processState.stageStats.sumCurrent += data.current;

    try {
        await storeLog(data);
    } catch (error) {
        console.error('[SUPABASE] Gagal menyimpan data sensor:', error.message);
    }

    const stage = stages[data.stageId];
    // Saat PAUSE atau EMERGENCY, pembacaan tetap direkam tetapi tidak dianggap sebagai
    // kegagalan set point proses karena aktuator memang sengaja dihentikan.
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
    client.publish(config.controlTopic, JSON.stringify(command), { qos: 1, retain: false });
    console.log(`[MQTT] Control dikirim: ${command.action}`);
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
    }
    if (command.action === 'set_actuator' && processState.latestData) {
        processState.latestData.act[command.actuator] = Boolean(command.enabled);
    }
}

const app = express();
app.disable('x-powered-by');
app.use((request, response, next) => {
    const origin = request.get('origin');
    if (origin && (!config.frontendOrigin || config.frontendOrigin === origin)) {
        response.set('Access-Control-Allow-Origin', origin);
        response.set('Vary', 'Origin');
    }
    response.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    response.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (request.method === 'OPTIONS') return response.sendStatus(204);
    return next();
});
app.use(express.json({ limit: '100kb' }));

app.get('/health', (_request, response) => {
    response.json({ status: 'ok', mqttConnected: Boolean(client?.connected), ...getPublicState() });
});

app.post('/api/auth/login', async (request, response) => {
    const { email, password, telegramId } = request.body || {};
    if (!email || !password || !telegramId) {
        return response.status(400).json({ error: 'Email, password, dan Telegram Chat ID wajib diisi.' });
    }
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    if (error || !data.session || !data.user) return response.status(401).json({ error: 'Email atau password tidak sesuai.' });

    const telegramChatId = String(telegramId).trim();
    const { data: profile, error: profileError } = await supabase
        .from('profiles').select('telegram_chat_id').eq('id', data.user.id).maybeSingle();
    if (profileError) return response.status(500).json({ error: `Profil tidak dapat dibaca: ${profileError.message}` });
    if (profile?.telegram_chat_id && profile.telegram_chat_id !== telegramChatId) {
        return response.status(403).json({ error: 'Telegram Chat ID tidak sesuai dengan akun ini.' });
    }
    const { error: upsertError } = await supabase.from('profiles').upsert({
        id: data.user.id,
        telegram_chat_id: telegramChatId,
        updated_at: new Date().toISOString()
    }, { onConflict: 'id' });
    if (upsertError) return response.status(500).json({ error: `Profil tidak dapat disimpan: ${upsertError.message}` });

    processState.notificationChatIds.add(telegramChatId);
    return response.json({
        accessToken: data.session.access_token,
        user: { email: data.user.email, telegramChatId }
    });
});

app.get('/api/state', requireUser, (_request, response) => response.json(getPublicState()));

app.get('/api/events', requireUser, (request, response) => {
    response.status(200).set({
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive'
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
    const limit = Math.min(Math.max(Number(request.query.limit) || 300, 1), 2_000);
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

app.post('/api/control', requireUser, async (request, response) => {
    const body = request.body || {};
    const allowedActions = new Set(['start', 'pause', 'resume', 'restart', 'emergency_stop', 'reset_emergency', 'set_actuator']);
    if (!allowedActions.has(body.action)) return response.status(400).json({ error: 'Aksi kontrol tidak dikenal.' });
    if (body.action === 'set_actuator' && !['motor', 'heater', 'catalyst_valve', 'separator_valve'].includes(body.actuator)) {
        return response.status(400).json({ error: 'Aktuator tidak dikenal.' });
    }

    const batchId = String(body.batch_id || processState.batchId || createAutomaticBatchId()).trim();
    const batchCode = String(body.batch_code || processState.batchCode || batchId).trim();
    const command = {
        action: body.action,
        batch_id: batchId,
        batch_code: batchCode,
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
    console.log(`[MQTT] Terhubung. Subscribe ${config.sensorTopic}`);
    client.subscribe(config.sensorTopic, { qos: 1 }, (error) => {
        if (error) console.error('[MQTT] Subscribe gagal:', error.message);
    });
});
client.on('reconnect', () => console.warn('[MQTT] Menghubungkan ulang...'));
client.on('error', (error) => console.error('[MQTT] Error:', error.message));

let messageQueue = Promise.resolve();
client.on('message', (_topic, message) => {
    messageQueue = messageQueue.then(() => processSensorData(JSON.parse(message.toString())))
        .catch((error) => console.error('[MQTT] Pesan gagal diproses:', error.message));
});

app.listen(config.port, '0.0.0.0', () => {
    console.log(`[HTTP] Dashboard/API siap di http://localhost:${config.port}`);
});
