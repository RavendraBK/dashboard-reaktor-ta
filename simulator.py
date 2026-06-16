import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import time
import json
import random

# ==========================================
# PENGATURAN MQTT
# ==========================================
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "ta/reaktor/data_sensor"

# Fungsi saat berhasil terkoneksi ke Broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Terhubung ke MQTT Broker HiveMQ!")
    else:
        print(f"❌ Gagal terhubung, kode error: {rc}")

# Inisialisasi Klien MQTT (DITAMBAHKAN CallbackAPIVersion.VERSION1)
client = mqtt.Client(CallbackAPIVersion.VERSION1, "Sim_ESP32_" + str(random.randint(1000, 9999)))
client.on_connect = on_connect

print("Mencoba terhubung ke Broker...")
client.connect(BROKER, PORT, 60)
client.loop_start()

# Variabel awal
stage = 1
counter = 0

print("🚀 Memulai simulasi pengiriman data ESP32 ke Dashboard...")
print("Tekan Ctrl+C untuk menghentikan.\n")

try:
    while True:
        counter += 1
        if counter % 10 == 0:
            stage += 1
            if stage > 6: stage = 1
            print(f"\n---> BERPINDAH KE STAGE {stage} <---")
        
        target_temp = 60 if stage == 1 else (100 if stage == 3 else (120 if stage == 5 else (0 if stage == 4 else 30)))
        target_rpm = 250 if stage in [1, 3, 5] else 0
        target_vol = 500 if stage in [1, 3, 4, 5] else 0
        
        temp = max(0, target_temp + random.uniform(-2.0, 2.0))
        rpm = max(0, target_rpm + random.randint(-5, 5))
        vol = max(0, target_vol + random.uniform(-5.0, 5.0))
        
        motor = stage in [1, 2, 3, 5]
        heater = stage in [1, 3, 5]
        valve = stage in [2, 6]

        if random.random() > 0.9 and target_temp > 0:
            temp += 20.0
            print("⚠️ [ANOMALI SUHU DIBUAT]")

        payload = {
            "temp": round(temp, 1),
            "rpm": int(rpm),
            "vol": round(vol, 1),
            "stage": stage,
            "act": { "motor": motor, "heater": heater, "valve": valve }
        }

        client.publish(TOPIC, json.dumps(payload))
        print(f"📤 Mengirim: {json.dumps(payload)}")
        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Simulasi dihentikan.")
    client.loop_stop()
    client.disconnect()