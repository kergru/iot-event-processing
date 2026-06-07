import json
import os
import random
import socket
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "sensors/raspberrypi/bmp180")
DEVICE_ID = os.environ.get("DEVICE_ID", socket.gethostname())
SENSOR = os.environ.get("SENSOR", "BMP180")
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "5"))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_start()

print(f"Publishing to {MQTT_BROKER}:{MQTT_PORT} on {MQTT_TOPIC}")

temperature = 22.0
pressure = 1012.0

try:
    while True:
        temperature += random.uniform(-0.2, 0.2)
        pressure += random.uniform(-0.5, 0.5)

        event = {
            "deviceId": DEVICE_ID,
            "sensor": SENSOR,
            "temperatureC": round(temperature, 2),
            "pressureHpa": round(pressure, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        payload = json.dumps(event)
        client.publish(MQTT_TOPIC, payload, qos=1)
        print(payload)

        time.sleep(INTERVAL_SECONDS)
except KeyboardInterrupt:
    pass
finally:
    client.loop_stop()
    client.disconnect()
