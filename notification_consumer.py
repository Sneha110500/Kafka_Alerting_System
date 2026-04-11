from config import CONSUMER_CONFIG, ALERTS_TOPIC
from confluent_kafka import Consumer, KafkaException
import requests
import json
import os
os.environ.pop("SSLKEYLOGFILE", None)

# ── Your Slack Webhook URL ───────────────────────────────────
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

# ── Format and send Slack notification ──────────────────────


def send_slack_notification(alert):
    emoji = "🚨" if alert["alert_type"] == "CRITICAL" else "⚠️"

    message = {
        "text": (
            f"{emoji} *{alert['alert_type']} ALERT*\n"
            f"*Server:*    {alert['server_id']}\n"
            f"*Metric:*    {alert['metric']}\n"
            f"*Value:*     {alert['value']}%\n"
            f"*Threshold:* {alert['threshold']}%\n"
            f"*Time:*      {alert['timestamp']}"
        )
    }

    response = requests.post(SLACK_WEBHOOK_URL, json=message)

    if response.status_code == 200:
        print(f"📱 Slack notified → {alert['alert_type']} "
              f"on {alert['server_id']}")
    else:
        print(f"❌ Slack failed → {response.status_code}")

# ── Main consumer loop ───────────────────────────────────────


def main():
    consumer_config = CONSUMER_CONFIG.copy()
    consumer_config["group.id"] = "notification-service"

    consumer = Consumer(consumer_config)
    consumer.subscribe([ALERTS_TOPIC])

    print("📱 Notification Consumer started — watching alerts topic")
    print("   Press Ctrl+C to stop\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            alert = json.loads(msg.value().decode("utf-8"))

            print(f"🔔 Alert received → {alert['alert_type']} | "
                  f"{alert['server_id']} | "
                  f"{alert['metric']}: {alert['value']}%")

            # Only send Slack for CRITICAL alerts
            # WARNING alerts just get logged
            if alert["alert_type"] == "CRITICAL":
                send_slack_notification(alert)
            else:
                print(f"⚠️  WARNING logged → {alert['server_id']} | "
                      f"{alert['metric']}: {alert['value']}%")

    except KeyboardInterrupt:
        print("\n⛔ Notification Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
