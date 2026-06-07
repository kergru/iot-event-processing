import json
import os
import socket
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
from smbus2 import SMBus

BMP180_ADDR = int(os.environ.get("BMP180_ADDR", "0x77"), 0)
BMP180_BUS = int(os.environ.get("BMP180_BUS", "1"))

MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.178.23")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "sensors/raspberrypi/bmp180")
MQTT_CONNECT_RETRY_SECONDS = float(os.environ.get("MQTT_CONNECT_RETRY_SECONDS", "5"))
DEVICE_ID = os.environ.get("DEVICE_ID", socket.gethostname())
SENSOR = os.environ.get("SENSOR", "BMP180")
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "5"))
OVERSAMPLING = int(os.environ.get("BMP180_OVERSAMPLING", "0"))


class Bmp180:
    def __init__(self, bus, address=BMP180_ADDR, oversampling=OVERSAMPLING):
        if oversampling < 0 or oversampling > 3:
            raise ValueError("BMP180_OVERSAMPLING must be between 0 and 3")
        self.bus = bus
        self.address = address
        self.oversampling = oversampling
        self.ac1 = self._read_s16(0xAA)
        self.ac2 = self._read_s16(0xAC)
        self.ac3 = self._read_s16(0xAE)
        self.ac4 = self._read_u16(0xB0)
        self.ac5 = self._read_u16(0xB2)
        self.ac6 = self._read_u16(0xB4)
        self.b1 = self._read_s16(0xB6)
        self.b2 = self._read_s16(0xB8)
        self.mb = self._read_s16(0xBA)
        self.mc = self._read_s16(0xBC)
        self.md = self._read_s16(0xBE)

    def read(self):
        raw_temperature = self._read_raw_temperature()
        raw_pressure = self._read_raw_pressure()

        x1 = ((raw_temperature - self.ac6) * self.ac5) >> 15
        x2 = (self.mc << 11) // (x1 + self.md)
        b5 = x1 + x2
        temperature_c = ((b5 + 8) >> 4) / 10.0

        b6 = b5 - 4000
        x1 = (self.b2 * ((b6 * b6) >> 12)) >> 11
        x2 = (self.ac2 * b6) >> 11
        x3 = x1 + x2
        b3 = (((self.ac1 * 4 + x3) << self.oversampling) + 2) >> 2
        x1 = (self.ac3 * b6) >> 13
        x2 = (self.b1 * ((b6 * b6) >> 12)) >> 16
        x3 = ((x1 + x2) + 2) >> 2
        b4 = (self.ac4 * (x3 + 32768)) >> 15
        b7 = (raw_pressure - b3) * (50000 >> self.oversampling)

        if b7 < 0x80000000:
            pressure_pa = (b7 * 2) // b4
        else:
            pressure_pa = (b7 // b4) * 2

        x1 = (pressure_pa >> 8) * (pressure_pa >> 8)
        x1 = (x1 * 3038) >> 16
        x2 = (-7357 * pressure_pa) >> 16
        pressure_pa = pressure_pa + ((x1 + x2 + 3791) >> 4)
        pressure_hpa = pressure_pa / 100.0

        return round(temperature_c, 2), round(pressure_hpa, 2)

    def _read_raw_temperature(self):
        self.bus.write_byte_data(self.address, 0xF4, 0x2E)
        time.sleep(0.005)
        msb = self.bus.read_byte_data(self.address, 0xF6)
        lsb = self.bus.read_byte_data(self.address, 0xF7)
        return (msb << 8) + lsb

    def _read_raw_pressure(self):
        self.bus.write_byte_data(self.address, 0xF4, 0x34 + (self.oversampling << 6))
        time.sleep({0: 0.005, 1: 0.008, 2: 0.014, 3: 0.026}[self.oversampling])
        msb = self.bus.read_byte_data(self.address, 0xF6)
        lsb = self.bus.read_byte_data(self.address, 0xF7)
        xlsb = self.bus.read_byte_data(self.address, 0xF8)
        return ((msb << 16) + (lsb << 8) + xlsb) >> (8 - self.oversampling)

    def _read_u16(self, register):
        msb = self.bus.read_byte_data(self.address, register)
        lsb = self.bus.read_byte_data(self.address, register + 1)
        return (msb << 8) + lsb

    def _read_s16(self, register):
        value = self._read_u16(register)
        return value - 65536 if value > 32767 else value


def mqtt_endpoint():
    if "://" not in MQTT_BROKER:
        return MQTT_BROKER, MQTT_PORT

    parsed = urlparse(MQTT_BROKER)
    if parsed.scheme not in ("mqtt", "tcp"):
        raise ValueError("MQTT_BROKER must be a host name, mqtt://host, or tcp://host")
    if not parsed.hostname:
        raise ValueError("MQTT_BROKER must include a host")
    return parsed.hostname, parsed.port or MQTT_PORT


def connect_mqtt(client):
    host, port = mqtt_endpoint()
    while True:
        try:
            print(f"Connecting to MQTT broker {host}:{port}")
            client.connect(host, port, 60)
            client.loop_start()
            print(f"Publishing to {host}:{port} on {MQTT_TOPIC}")
            return
        except OSError as exception:
            print(
                f"MQTT connection to {host}:{port} failed: {exception}. "
                f"Retrying in {MQTT_CONNECT_RETRY_SECONDS:g}s."
            )
            time.sleep(MQTT_CONNECT_RETRY_SECONDS)


def build_event(temperature_c, pressure_hpa):
    return {
        "deviceId": DEVICE_ID,
        "sensor": SENSOR,
        "temperatureC": temperature_c,
        "pressureHpa": pressure_hpa,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    bus = SMBus(BMP180_BUS)
    sensor = Bmp180(bus)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    connect_mqtt(client)

    try:
        while True:
            temperature_c, pressure_hpa = sensor.read()
            event = build_event(temperature_c, pressure_hpa)
            payload = json.dumps(event)
            client.publish(MQTT_TOPIC, payload, qos=1)
            print(payload)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        bus.close()


if __name__ == "__main__":
    main()
