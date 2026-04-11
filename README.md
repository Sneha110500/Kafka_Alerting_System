# Kafka_Alerting_System
Real time infrastructure alerting pipeline built with Apache Kafka and Confluent Cloud.

# Real-Time Infrastructure Alerting System

A real-time server monitoring and alerting pipeline built with Apache Kafka and Confluent Cloud. Detects dangerous server metrics and sends instant Slack notifications.

## What It Does

Simulates 5 servers continuously publishing CPU, memory, and disk metrics every second. An alert consumer monitors these metrics against thresholds and fires real-time Slack notifications when servers are under stress.

-> Producer → system-metrics topic → Alert Consumer → alerts topic → Notification Consumer → Slack DM


##  Architecture

┌─────────────────────────────────────────────────────┐
│                                                     │
│  producer.py                                        │
│  Simulates 5 servers publishing metrics/second      │
│  Key = server_id (ordering guaranteed per server)   │
│              │                                      │
│              ▼                                      │
│  Confluent Cloud (Managed Kafka Cluster)            │
│  ├── Topic: system-metrics (3 partitions)           │
│  └── Topic: alerts (3 partitions)                   │
│              │                                      │
│              ▼                                      │
│  alert_consumer.py (Consumer Group: alert-processors)│
│  Applies threshold rules → publishes to alerts topic│
│              │                                      │
│              ▼                                      │
│  notification_consumer.py (Consumer Group:          │
│  notification-service)                              │
│  Sends real-time Slack notifications                │
│              │                                      │
│              ▼                                      │
│         📱 Slack DM — Instant Alert!                │
│                                                     │
└─────────────────────────────────────────────────────┘


## Alert Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| CPU % | > 60% | > 80% |
| Memory % | > 55% | > 75% |
| Disk % | > 75% | > 90% |


##  Kafka Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| Topics & Partitions | 2 topics, 3 partitions each |
| Key-based routing | server_id as partition key |
| Consumer Groups | alert-processors & notification-service |
| acks=all | Zero data loss guaranteed |
| Schema Registry | JSON schema enforced on system-metrics |
| Fault Tolerance | Tested consumer crash & restart |
| Offset management | Consumers resume from last committed offset |


## Technologies

- **Apache Kafka** — Event streaming platform
- **Confluent Cloud** — Managed Kafka cluster
- **Python** — Producer and consumer clients
- **Slack Webhooks** — Real-time notifications
- **Schema Registry** — Message structure enforcement


## How to Run
### Prerequisites
- Python 3.8+
- Confluent Cloud account (free tier)
- Slack workspace with incoming webhook

### Setup

**1. Clone the repository:**
```bash
git clone https://github.com/Sneha110500/Kafka_Alerting_System.git
cd Kafka_Alerting_System
```

**2. Install dependencies:**
```bash
pip install confluent-kafka requests
```

**3. Configure credentials:**
```bash
cp config_template.py config.py
# Edit config.py with your Confluent Cloud credentials
```

**4. Create Confluent Cloud topics:**
- `system-metrics` (3 partitions)
- `alerts` (3 partitions)

**5. Run the pipeline (3 separate terminals):**
```bash
# Terminal 1 — Start producer
py producer.py

# Terminal 2 — Start alert consumer
py alert_consumer.py

# Terminal 3 — Start notification consumer
py notification_consumer.py
```


## 📊 Sample Output

```
📤 Publishing → server-4 | CPU: 93.8% | Memory: 77.0% | Disk: 94.4%
🚨 CRITICAL ALERT → server-4 | cpu_percent: 93.8% (threshold: 80%)
✅ Alert published → Partition: 1, Offset: 2792
📱 Slack notified → CRITICAL on server-4
```


## Fault Tolerance Testing

Tested consumer crash and recovery:
- Killed alert_consumer mid-stream
- Last processed offset: 6867
- Restarted consumer → resumed from offset: 6868
- Zero messages lost ✅


## Project Structure

kafka-alerting-system/
├── producer.py              # Metric simulator & Kafka producer
├── alert_consumer.py        # Rules engine & alert publisher  
├── notification_consumer.py # Slack notification sender
├── config_template.py       # Configuration template
├── requirements.txt         # Python dependencies
└── .gitignore              # Excludes sensitive config.py
