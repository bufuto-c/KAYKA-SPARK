# producer_sensors.py
import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_ids = ['sensor-A', 'sensor-B', 'sensor-C']
states = ['OK', 'WARN', 'ERROR', None]

def gen_event(i):
    # Introducimos aleatoriamente errores/nulos
    sensor = random.choice(sensor_ids)
    base = {
        "sensor_id": sensor,
        # temperature: a veces float, a veces string mal formado, a veces None
        "temperature": random.choice([round(random.uniform(15.0, 35.0),2),
                                      str(round(random.uniform(15.0,35.0),2)), None]),
        # humidity: a veces integer, sometimes string "n/a"
        "humidity": random.choice([random.randint(20, 90), "n/a", None]),
        # vibration (numeric), but sometimes missing
        # state machine: active / inactive / None
        "state": random.choice(states),
        # timestamp: sometimes correct ISO, sometimes epoch as string, sometimes malformed
        "ts": random.choice([
            datetime.now(timezone.utc).isoformat(),
            str(int(time.time())),            # epoch seconds as string
            "BAD_TIMESTAMP"                    # malformed
        ])
    }
    # Occasionally drop the 'vibration' field entirely to simulate missing field
    if random.random() > 0.25:
        base["vibration"] = round(random.uniform(0.0, 5.0), 3)
    # Occasionally add a noisy field
    if random.random() > 0.8:
        base["extra"] = "noise"
    base["seq"] = i
    return base

def send(n=50, delay=0.2):
    for i in range(n):
        ev = gen_event(i)
        producer.send('sensor-events', ev)
        print(f"📤 Sent: {ev}")
        time.sleep(delay)
    producer.flush()
    print("Done sending.")

if __name__ == "__main__":
    send(n=200, delay=0.05)
