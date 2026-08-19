"""Simulator ESP32 untuk dashboard reaktor esterifikasi.

Jalankan setelah backend aktif:
    py -m pip install -r requirements.txt
    py simulator.py

Simulator menunggu perintah Start Simulation dari dashboard. Semua tombol kontrol
(pause, resume, restart, emergency stop, dan toggle aktuator) dikirim melalui MQTT.
"""

import json
import os
import random
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Fix encoding konsol Windows untuk karakter emoji
if sys.platform == "win32":
    import io
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import paho.mqtt.client as mqtt

try:
    from paho.mqtt.enums import CallbackAPIVersion
except ImportError:
    CallbackAPIVersion = None



# ==========================================
# PENGATURAN MQTT — samakan dengan file .env
# ==========================================
BROKER = os.getenv("MQTT_BROKER", "broker.emqx.io")
PORT = int(os.getenv("MQTT_PORT", "1883"))
SENSOR_TOPIC = os.getenv("MQTT_SENSOR_TOPIC", "ta/reaktor/data_sensor")
CONTROL_TOPIC = os.getenv("MQTT_CONTROL_TOPIC", "ta/reaktor/control")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "2"))
STATE_FILE = Path(__file__).with_name("simulator_state.json")


def new_batch_code():
    return "SIM-" + datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d-%H%M%S")


state_lock = threading.Lock()
state = {
    "run_status": "standby",  # standby | running | paused | emergency
    "stage": 1,
    "sample_count": 0,
    "stage_sample_count": 0,
    "elapsed_seconds": 0,
    "batch_id": None,
    "batch_code": None,
    # Nilai pada dict ini hanya ada bila user menekan toggle di dashboard.
    "manual_actuators": {},
    "last_values": {"temp": 28.0, "rpm": 0, "vol": 0.0, "current": 0.0}
}


def save_state():
    """Menyimpan progress agar PC/simulator bisa dimatikan lalu dilanjutkan esok hari."""
    with state_lock:
        saved = {
            "stage": state["stage"],
            "sample_count": state["sample_count"],
            "stage_sample_count": state["stage_sample_count"],
            "elapsed_seconds": state["elapsed_seconds"],
            "batch_id": state["batch_id"],
            "batch_code": state["batch_code"],
            "manual_actuators": state["manual_actuators"],
            "last_values": state["last_values"]
        }
    try:
        STATE_FILE.write_text(json.dumps(saved, indent=2), encoding="utf-8")
    except OSError as error:
        print("⚠️  Progress simulator tidak dapat disimpan:", error)


def restore_state():
    """Memulihkan progress, tetapi selalu menunggu tombol Resume demi keselamatan."""
    if not STATE_FILE.exists():
        return
    try:
        saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        with state_lock:
            for key in ("stage", "sample_count", "stage_sample_count", "elapsed_seconds", "batch_id", "batch_code", "manual_actuators", "last_values"):
                if key in saved:
                    state[key] = saved[key]
            if state["batch_id"]:
                state["run_status"] = "paused"
        print(f"💾 Progress batch {state['batch_code']} dipulihkan pada stage {state['stage']}. Tekan Resume di dashboard untuk melanjutkan.")
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as error:
        print("⚠️  File progress simulator tidak dapat dibaca, mulai dari kondisi baru:", error)


def base_actuators(stage):
    """Kondisi aktuator normal pada masing-masing stage."""
    return {
        1: {"motor": True, "heater": True, "catalyst_valve": False, "separator_valve": False},
        2: {"motor": False, "heater": False, "catalyst_valve": True, "separator_valve": False},
        3: {"motor": False, "heater": True, "catalyst_valve": False, "separator_valve": False},
        4: {"motor": False, "heater": False, "catalyst_valve": False, "separator_valve": False},
        5: {"motor": True, "heater": True, "catalyst_valve": False, "separator_valve": False},
        6: {"motor": False, "heater": False, "catalyst_valve": False, "separator_valve": True},
    }[stage]


def reset_batch(command):
    state["stage"] = 1
    state["sample_count"] = 0
    state["stage_sample_count"] = 0
    state["elapsed_seconds"] = 0
    state["batch_id"] = str(command.get("batch_id") or new_batch_code())
    state["batch_code"] = str(command.get("batch_code") or state["batch_id"])
    state["manual_actuators"] = {}
    state["last_values"] = {"temp": 28.0, "rpm": 0, "vol": 500.0, "current": 0.0}


def make_payload(add_anomaly=False):
    """Membuat satu payload seperti yang kelak dikirim ESP32."""
    with state_lock:
        stage = state["stage"]
        actuators = base_actuators(stage).copy()
        actuators.update(state["manual_actuators"])
        motor_on = actuators["motor"]
        heater_on = actuators["heater"]

        # Suhu sekitar hanya dipakai jika heater OFF. Reflux memang RPM 0.
        target_temp = {1: 60, 3: 100, 5: 120}.get(stage, 28) if heater_on else 28
        target_rpm = 250 if motor_on and stage in (1, 5) else 0
        target_current = 2.0 if motor_on else 0.0

        if stage == 6:
            # Meniru beaker hasil akhir yang terisi selama filtrasi.
            volume = min(500, 300 + state["stage_sample_count"] * 12) + random.uniform(-2, 2)
        else:
            volume = 500 + random.uniform(-5, 5)

        temp = max(0, target_temp + random.uniform(-2, 2))
        rpm = max(0, int(round(target_rpm + random.uniform(-5, 5)))) if target_rpm else 0
        current = max(0, target_current + random.uniform(-0.15, 0.15)) if motor_on else 0.0

        if add_anomaly:
            # Bergantian membuat pelanggaran yang mudah terlihat dan memicu Telegram.
            if heater_on:
                temp += 20
                print("⚠️  Anomali suhu simulasi dibuat.")
            elif motor_on:
                current = 4.0
                print("⚠️  Anomali arus simulasi dibuat.")
            else:
                current = 0.5
                print("⚠️  Anomali arus saat motor OFF dibuat.")

        level_detected = stage == 6 and volume >= 375
        actuators["valve"] = actuators["catalyst_valve"] or actuators["separator_valve"]
        state["last_values"] = {
            "temp": round(temp, 1), "rpm": rpm, "vol": round(max(0, volume), 1),
            "current": round(current, 2)
        }
        return {
            **state["last_values"],
            "stage": stage,
            "batch_id": state["batch_id"],
            "batch_code": state["batch_code"],
            "elapsed_seconds": state["elapsed_seconds"],
            "level_detected": level_detected,
            "machine_status": state["run_status"],
            "act": actuators
        }


def publish_snapshot(force_anomaly=False):
    payload = make_payload(add_anomaly=force_anomaly)
    result = client.publish(SENSOR_TOPIC, json.dumps(payload), qos=1)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print("📤 Mengirim:", json.dumps(payload, ensure_ascii=False))
    else:
        print("❌ Payload belum terkirim. Kode MQTT:", result.rc)


def on_connect(mqtt_client, _userdata, _flags, reason_code, _properties=None):
    rc = getattr(reason_code, "value", reason_code)
    if rc == 0:
        print("✅ Terhubung ke MQTT Broker HiveMQ.")
        mqtt_client.subscribe(CONTROL_TOPIC, qos=1)
        print(f"👂 Menunggu perintah dashboard pada: {CONTROL_TOPIC}")
    else:
        print(f"❌ Gagal terhubung ke broker. Kode: {rc}")


def on_message(_mqtt_client, _userdata, message):
    try:
        command = json.loads(message.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        print("❌ Perintah kontrol bukan JSON yang valid.")
        return

    action = command.get("action")
    with state_lock:
        if action in ("start", "restart"):
            reset_batch(command)
            state["run_status"] = "running"
            print(f"▶️  {action.capitalize()} batch {state['batch_code']}.")
        elif action == "pause" and state["run_status"] == "running":
            state["run_status"] = "paused"
            print("⏸️  Simulasi dijeda; timer tidak bertambah.")
        elif action == "resume":
            if not state["batch_id"]:
                # Berguna jika backend dan simulator baru dinyalakan. Isi kode batch
                # yang lama di dashboard sebelum menekan Resume.
                reset_batch(command)
            state["run_status"] = "running"
            print("▶️  Simulasi dilanjutkan.")
        elif action == "emergency_stop":
            state["run_status"] = "emergency"
            state["manual_actuators"] = {
                "motor": False, "heater": False, "catalyst_valve": False, "separator_valve": False
            }
            print("🛑 EMERGENCY STOP diterima; semua aktuator simulator dimatikan.")
        elif action == "reset_emergency" and state["run_status"] == "emergency":
            state["run_status"] = "paused"
            print("🔓 Emergency di-reset; simulator tetap PAUSE sampai Resume ditekan.")
        elif action == "set_actuator":
            actuator = command.get("actuator")
            if actuator in ("motor", "heater", "catalyst_valve", "separator_valve"):
                state["manual_actuators"][actuator] = bool(command.get("enabled"))
                print(f"🎚️  Toggle {actuator} = {bool(command.get('enabled'))}.")
            else:
                print("❌ Nama aktuator dari dashboard tidak dikenal.")
        else:
            print(f"ℹ️  Perintah diabaikan: {action}")
            return

    save_state()
    # Mengirim satu status langsung agar dashboard segera menampilkan hasil tombol.
    publish_snapshot()


if CallbackAPIVersion is not None:
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="Sim_ESP32_" + str(random.randint(1000, 9999)),
        protocol=mqtt.MQTTv311
    )
else:
    client = mqtt.Client(
        client_id="Sim_ESP32_" + str(random.randint(1000, 9999)),
        protocol=mqtt.MQTTv311
    )
client.on_connect = on_connect
client.on_message = on_message



def main():
    restore_state()
    print("Mencoba terhubung ke broker MQTT...")
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()
    print("🚀 Simulator siap. Tekan Start Simulation pada dashboard.")
    print("Tekan Ctrl+C di jendela ini untuk menghentikan simulator.\n")

    try:
        while True:
            with state_lock:
                running = state["run_status"] == "running"
                if running:
                    state["sample_count"] += 1
                    state["stage_sample_count"] += 1
                    state["elapsed_seconds"] += int(PUBLISH_INTERVAL_SECONDS)
                    if state["sample_count"] % 10 == 0:
                        state["stage"] = 1 if state["stage"] == 6 else state["stage"] + 1
                        state["stage_sample_count"] = 0
                        print(f"\n---> BERPINDAH KE STAGE {state['stage']} <---")

            if running:
                # Peluang kecil untuk menguji banner anomali dan Telegram tanpa terlalu spam.
                publish_snapshot(force_anomaly=random.random() > 0.94)
                save_state()
            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Simulator dihentikan.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
