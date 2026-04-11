import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaException
from config import CONSUMER_CONFIG, PRODUCER_CONFIG, METRICS_TOPIC, ALERTS_TOPIC

# ── Alert thresholds ─────────────────────────────────────────
THRESHOLDS = {
    "cpu_percent":    {"warning": 60, "critical": 80},
    "memory_percent": {"warning": 55, "critical": 75},
    "disk_percent":   {"warning": 75, "critical": 90},
}

# ── Check metrics against thresholds ────────────────────────


def check_thresholds(metrics):
    alerts = []

    for metric, levels in THRESHOLDS.items():
        value = metrics[metric]

        if value >= levels["critical"]:
            alerts.append({
                "server_id":  metrics["server_id"],
                "alert_type": "CRITICAL",
                "metric":     metric,
                "value":      value,
                "threshold":  levels["critical"],
                "timestamp":  datetime.now(timezone.utc).isoformat()
            })
        elif value >= levels["warning"]:
            alerts.append({
                "server_id":  metrics["server_id"],
                "alert_type": "WARNING",
                "metric":     metric,
                "value":      value,
                "threshold":  levels["warning"],
                "timestamp":  datetime.now(timezone.utc).isoformat()
            })

    return alerts

# ── Delivery confirmation ────────────────────────────────────


def delivery_report(err, msg):
    if err:
        print(f"❌ Alert delivery failed: {err}")
    else:
        print(f"✅ Alert published → Partition: "
              f"{msg.partition()}, Offset: {msg.offset()}")

# ── Main consumer loop ───────────────────────────────────────


def main():
    # Add group.id to consumer config
    consumer_config = CONSUMER_CONFIG.copy()
    consumer_config["group.id"] = "alert-processors"

    consumer = Consumer(consumer_config)
    producer = Producer(PRODUCER_CONFIG)

    consumer.subscribe([METRICS_TOPIC])

    print("🔍 Alert Consumer started — watching for dangerous metrics")
    print("   Press Ctrl+C to stop\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # Parse the message
            metrics = json.loads(msg.value().decode("utf-8"))
            print(
                f"📍 Reading → Partition: {msg.partition()}, Offset: {msg.offset()}")

            print(f"📥 Received → {metrics['server_id']} | "
                  f"CPU: {metrics['cpu_percent']}% | "
                  f"Memory: {metrics['memory_percent']}% | "
                  f"Disk: {metrics['disk_percent']}%")

            # Check against thresholds
            alerts = check_thresholds(metrics)

            if alerts:
                for alert in alerts:
                    print(f"🚨 {alert['alert_type']} ALERT → "
                          f"{alert['server_id']} | "
                          f"{alert['metric']}: {alert['value']}% "
                          f"(threshold: {alert['threshold']}%)")

                    # Publish alert to alerts topic
                    producer.produce(
                        topic=ALERTS_TOPIC,
                        key=alert["server_id"],
                        value=json.dumps(alert),
                        on_delivery=delivery_report
                    )
                    producer.flush()
            else:
                print(f"✅ {metrics['server_id']} — all metrics healthy")

    except KeyboardInterrupt:
        print("\n⛔ Alert Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
