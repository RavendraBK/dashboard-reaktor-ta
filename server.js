// ==========================================
// BACKEND WORKER: REAKTOR IoT
// Berjalan 24/7 di latar belakang
// ==========================================

const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const express = require('express'); // Tambahan baru untuk Render.com

// ==========================================
// 1. SETUP SERVER HTTP (UNTUK UPTIMEROBOT)
// ==========================================
const app = express();
const PORT = process.env.PORT || 3000;

// Membuat pintu masuk web sederhana agar UptimeRobot bisa "mencolek" server ini
app.get('/', (req, res) => {
    res.send("✅ Backend Reaktor IoT Berjalan Normal 24/7!");
});

app.listen(PORT, () => {
    console.log(`🌐 Server HTTP aktif di port ${PORT} (Siap di-ping UptimeRobot)`);
});

// ==========================================
// 2. KONFIGURASI KUNCI (GANTI DENGAN MILIK ANDA)
// ==========================================
const SUPABASE_URL = 'https://iretftgjvpljqyfixmow.supabase.co/'; // GANTI INI
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlyZXRmdGdqdnBsanF5Zml4bW93Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODE1MzU0OTQsImV4cCI6MjA5NzExMTQ5NH0.zD4LTLC2aV1J1oni7L7HicTM3x_RK5M73IrhImGZx70'; // GANTI INI
const TELEGRAM_BOT_TOKEN = '8810149825:AAFEsuOfhVgk8hGvc_C6h_2FO2i3kiPriUs'; // GANTI INI
const TELEGRAM_CHAT_ID = '8205817584'; // GANTI INI

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// 3. TARGET PARAMETER (Sama dengan di Frontend)
const targetParameters = {
    1: { name: "Mixing", targetTemp: 60, targetRpm: 250, targetVol: 500, checkTolerance: true },
    2: { name: "Add Catalyst", targetTemp: null, targetRpm: null, targetVol: null, checkTolerance: false },
    3: { name: "Reflux", targetTemp: 100, targetRpm: 250, targetVol: 500, checkTolerance: true },
    4: { name: "Separation", targetTemp: 0, targetRpm: 0, targetVol: 500, checkTolerance: true },
    5: { name: "Oil Treatment", targetTemp: 120, targetRpm: 250, targetVol: 500, checkTolerance: true },
    6: { name: "Filtration", targetTemp: null, targetRpm: null, targetVol: null, checkTolerance: false }
};

let lastNotifTime = 0;

// 4. FUNGSI KIRIM TELEGRAM
async function sendTelegramAlert(message) {
    const now = Date.now();
    if (now - lastNotifTime < 60000) return; 

    // Perbaikan: import fetch secara dinamis (mendukung Node.js lama dan baru)
    const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args)).catch(err => globalThis.fetch(...args));
    
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage?chat_id=${TELEGRAM_CHAT_ID}&text=${encodeURIComponent("🚨 *SYSTEM ALERT (BACKEND)*\n" + message)}`;
    
    try {
        await fetch(url);
        console.log(`[TELEGRAM SENT]: Peringatan terkirim!`);
        lastNotifTime = now;
    } catch (error) {
        console.error("Gagal mengirim Telegram:", error.message);
    }
}

// 5. KONEKSI KE MQTT BROKER
console.log("Menghubungkan ke MQTT Broker...");
const client = mqtt.connect('mqtt://broker.hivemq.com:1883'); 

client.on('connect', () => {
    console.log("✅ Berhasil terhubung ke Broker HiveMQ!");
    client.subscribe('ta/reaktor/data_sensor');
    console.log("📡 Menunggu data sensor dari mesin...");
});

// 6. LOGIKA SAAT DATA MASUK (SETIAP DETIK)
client.on('message', async (topic, message) => {
    try {
        const data = JSON.parse(message.toString());
        
        // A. Cek Anomali Toleransi
        const stage = targetParameters[data.stage];
        if (stage && stage.checkTolerance) {
            let errorMsg = [];
            if (stage.targetTemp > 0 && (data.temp < stage.targetTemp * 0.9 || data.temp > stage.targetTemp * 1.1)) {
                errorMsg.push(`Suhu ${data.temp}°C di luar batas.`);
            }
            if (stage.targetRpm > 0 && (data.rpm < stage.targetRpm * 0.9 || data.rpm > stage.targetRpm * 1.1)) {
                errorMsg.push(`RPM ${data.rpm} di luar batas.`);
            }
            
            if (errorMsg.length > 0) {
                sendTelegramAlert(`Peringatan Stage [${stage.name}]:\n${errorMsg.join('\n')}`);
            }
        }

        // B. Simpan ke Database Supabase Otomatis
        const logEntry = { 
            stage: data.stage,
            temp: data.temp,
            rpm: data.rpm,
            vol: data.vol,
            waktu_lokal: new Date().toLocaleTimeString('en-US'), 
            tanggal: new Date().toLocaleDateString('en-US') 
        };
        
        const { error } = await supabase.from('reactorlogs').insert([logEntry]);
        if (error) {
            console.error("Error simpan ke DB:", error.message);
        }

    } catch (err) {
        console.error("Error memproses pesan:", err.message);
    }
});