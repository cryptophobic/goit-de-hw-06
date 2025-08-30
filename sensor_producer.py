import json, time, random
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": '77.81.230.104:9092',
    "security.protocol": 'SASL_PLAINTEXT',
    "sasl.mechanism": 'PLAIN',
    "sasl.username": 'admin',
    "sasl.password": 'VawEzo1ikLtrA8Ug8THa',
    "enable.idempotence": True,
    "linger.ms": 5,
}
p = Producer(conf)

def now_iso(): return datetime.now(timezone.utc).isoformat()

i = 0
try:
    while True:
        payload = {
            "id": f"device-{i%5}",
            "temperature": round(random.uniform(15, 80), 2),
            "humidity": round(random.uniform(10, 95), 2),
            "timestamp": now_iso(),
        }
        p.produce('building_sensors_cryptophobic', json.dumps(payload).encode("utf-8"))
        p.poll(0)
        i += 1
        time.sleep(1.0 / 10)
except KeyboardInterrupt:
    pass
finally:
    p.flush()
