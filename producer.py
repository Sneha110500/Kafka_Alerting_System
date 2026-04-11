import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from config import PRODUCER_CONFIG, METRICS_TOPIC

# ── Delivery confirmation callback ──────────────────────────


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"✅ Metric delivered → Topic: {msg.topic()}, "
              f"Partition: {msg.partition()}, "
              f"Offset: {msg.offset()}")

# ── Simulate realistic server metrics ───────────────────────


def generate_metrics(server_id):
    # 80% chance normal, 20% chance spike (to trigger alerts)
    is_spike = random.random() < 0.2

    if is_spike:
        cpu = round(random.uniform(75, 99), 1)
        memory = round(random.uniform(70, 95), 1)
        disk = round(random.uniform(85, 99), 1)
    else:
        cpu = round(random.uniform(10, 60), 1)
        memory = round(random.uniform(20, 65), 1)
        disk = round(random.uniform(10, 70), 1)

    return {
        "server_id":        server_id,
        "cpu_percent":      cpu,
        "memory_percent":   memory,
        "disk_percent":     disk,
        "timestamp":        datetime.now(timezone.utc).isoformat()
    }

# ── Main producer loop ───────────────────────────────────────


def main():
    producer = Producer(PRODUCER_CONFIG)
    servers = ["server-1", "server-2", "server-3",
               "server-4", "server-5"]

    print("🚀 Producer started — publishing metrics every second")
    print("   Press Ctrl+C to stop\n")

    try:
        while True:
            for server_id in servers:
                metrics = generate_metrics(server_id)

                producer.produce(
                    topic=METRICS_TOPIC,
                    key=server_id,        # same server → same partition
                    value=json.dumps(metrics),
                    on_delivery=delivery_report
                )

                print(f"📤 Publishing → {server_id} | "
                      f"CPU: {metrics['cpu_percent']}% | "
                      f"Memory: {metrics['memory_percent']}% | "
                      f"Disk: {metrics['disk_percent']}%")

            producer.flush()  # wait for all messages to be delivered
            time.sleep(1)     # wait 1 second before next round

    except KeyboardInterrupt:
        print("\n⛔ Producer stopped.")
        producer.flush()


if __name__ == "__main__":
    main()
