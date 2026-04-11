# config_template.py
# Copy this to config.py and fill in your real credentials

BOOTSTRAP_SERVER = "your-bootstrap-server-here"
API_KEY = "your-api-key-here"
API_SECRET = "your-api-secret-here"

METRICS_TOPIC = "system-metrics"
ALERTS_TOPIC = "alerts"

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
    "acks":              "all"
}

CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
    "auto.offset.reset": "earliest"
}
