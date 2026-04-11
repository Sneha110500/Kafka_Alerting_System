# Confluent Cloud Configuration

BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"  # bootstrap server
API_KEY = "ZQ3GKEWASRYCM5XV"                              # API key
API_SECRET = "cfltrIz8C1wvPAk286D9fu4tFFM17Rjfm0iDR6y9foBt/FyDBY9H5s8kp4O7L8+w"    # API secret

# Topic Names
METRICS_TOPIC = "system-metrics"
ALERTS_TOPIC = "alerts"

# Producer Config
PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
    "acks":              "all"   # wait for all ISR replicas — no data loss
}

# Consumer Config
CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
    "auto.offset.reset": "earliest"  # read from beginning if no offset exists
}


# BOOTSTRAP_SERVER = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
# API_KEY = "2S2FGNHFLRL6JXK7"
# API_SECRET = "cflt772M7A7X7smXvQrKM3dQWmP8Y+iC4K3x+oO7HbWqvDW1nd9icodS+9INPUhg"
