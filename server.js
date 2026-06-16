const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

// --- KONFIGURASI (GANTI DENGAN DATA ASLI ANDA) ---
const SUPABASE_URL = 'https://iretftgjvpljqyfixmow.supabase.co/';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlyZXRmdGdqdnBsanF5Zml4bW93Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODE1MzU0OTQsImV4cCI6MjA5NzExMTQ5NH0.zD4LTLC2aV1J1oni7L7HicTM3x_RK5M73IrhImGZx70';
const TELEGRAM_TOKEN = '8810149825:AAFEsuOfhVgk8hGvc_C6h_2FO2i3kiPriUs';
const CHAT_ID = '8205817584';

// --- INISIALISASI ---
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const client = mqtt.connect('mqtt://broker.hivemq.com:1883');

client.on('connect', () => {
    console.log('✅ Berhasil terhubung ke Broker HiveMQ!');
    client.subscribe('ta/reaktor/data_sensor');
});

// Fungsi untuk kirim Telegram
async function sendTelegram(message) {
    try {
        await axios.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
            chat_id: CHAT_ID,
            text: message
        });
        console.log('[TELEGRAM SENT]: Peringatan terkirim!');
    } catch (error) {
        console.error('Gagal kirim Telegram:', error.message);
    }
}

// --- PROSES DATA ---
client.on('message', async (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());
        console.log('Menerima data dari MQTT:', payload);

        // 1. Simpan ke Supabase
        // Pastikan nama kolom di tabel reactor_logs di Supabase adalah (stage, temp, rpm, vol)
        const { error } = await supabase
            .from('reactor_logs')
            .insert([{
                stage: payload.stage,
                temp: payload.temp,
                rpm: payload.rpm,
                vol: payload.vol
            }]);

        if (error) {
            console.error('Error simpan ke Supabase:', error.message);
        } else {
            console.log('✅ Data berhasil masuk ke Supabase');
        }

        // 2. Cek Anomali untuk Telegram
        // Mengirim notifikasi jika suhu > 80
        if (payload.temp > 80) {
            await sendTelegram(`⚠️ PERINGATAN! Suhu Reaktor Tinggi: ${payload.temp}°C`);
        }

    } catch (err) {
        console.error('Gagal memproses pesan MQTT:', err.message);
    }
});

console.log('📡 Menunggu data sensor dari mesin...');