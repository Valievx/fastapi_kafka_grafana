from prometheus_client import Counter

kafka_messages_sent_total = Counter(
    "kafka_messages_sent_total",
    "Total number of Kafka messages sent by the producer",
    ["topic"],
)

kafka_messages_processed_total = Counter(
    "kafka_messages_processed_total",
    "Total number of Kafka messages processed by a consumer",
    ["topic", "consumer_id"],
)
