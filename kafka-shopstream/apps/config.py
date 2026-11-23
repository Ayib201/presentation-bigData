"""Configuration partagée par les applications ShopStream."""
import os

# Les trois listeners EXTERNAL exposés par docker-compose sur la machine hôte
BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "localhost:29092,localhost:39092,localhost:49092"
)

TOPIC_ORDERS = "orders.created"
TOPIC_ALERTS = "orders.fraud-alerts"
TOPIC_DLQ = "orders.dlq"

REQUIRED_FIELDS = (
    "order_id", "customer_id", "timestamp", "category", "amount",
    "card_country", "shipping_country",
)
