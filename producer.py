import socket
import time
from datetime import datetime, timezone
from pathlib import Path
import random

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka import SerializingProducer


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered: {msg.topic()} [{msg.partition()}] offset={msg.offset()}")


def main():
    topic = "demo"

    # 1) Schema Registry client
    schema_registry_conf = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 2) Load your cafe JSON schema
    schema_str = Path("cafe_event_schema.JSON").read_text()

    # 3) Serializer function: takes Python dict -> dict for JSONSerializer
    def to_dict(obj, ctx):
        return obj

    json_serializer = JSONSerializer(schema_str, schema_registry_client, to_dict)

    # 4) Producer configuration
    producer_conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "Marco's Awesome Cafe",
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": json_serializer,
        "acks": "1",  # Wait for leader acknowledgment
    }
    producer = SerializingProducer(producer_conf)

    # 5) Random data for 5 messages
    customers = ["Marco", "Tao", "Maher", "Becky", "Tiffany"]
    drinks = ["Matcha", "Hot Chocolate", "Latte", "Mocha", "Coffee"]

    for i in range(1, 6):
        payload = {
            "event_id": i,
            "customer_name": random.choice(customers),
            "drink_name": random.choice(drinks),
            "quantity": random.randint(1, 5),
            "amount": round(random.uniform(2.0, 20.0), 2),
            "is_member": random.choice([True, False]),
            "ts_utc": datetime.now(timezone.utc).isoformat(),
        }

        # Produce message with event_id as key
        producer.produce(
            topic=topic,
            key=None, # Removed key so that events are randomly assigned to partitions
            value=payload,
            on_delivery=delivery_report,
        )

        producer.poll(0)  # Trigger delivery callbacks
        time.sleep(0.2)

    # Flush remaining messages
    producer.flush(10)


if __name__ == "__main__":
    main()