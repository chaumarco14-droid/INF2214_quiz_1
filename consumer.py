from pathlib import Path

from confluent_kafka import DeserializingConsumer, KafkaException
from confluent_kafka.serialization import StringDeserializer, SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

def main():
    print("Consumer started, waiting for messages... (Ctrl+C to stop)")

    schema_str = Path("cafe_event_schema.JSON").read_text()
    schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})

    
    def from_dict(obj: dict, ctx: SerializationContext):
        return obj

    json_deserializer = JSONDeserializer(
        schema_str,
        from_dict=from_dict,
        schema_registry_client=schema_registry_client,
    )

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "demo-consumer",  # Set to demo-consumer for demonstration / testing purposes
            "auto.offset.reset": "earliest", # Set to earliest so that all 5 messages are consumed if an error happens
            "enable.auto.commit": True,  # Set auto commit to true since only 5 messages are consumed and they are sent manually so there's no need for fine-tuned committing. 
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": json_deserializer,
        }
    )

    consumer.subscribe(["demo"])
    print("Subscribed to topic: demo")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            
            value = msg.value()
            print(
                f"Consumed: topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()} key={msg.key()} "
                f"value={{event_id: {value['event_id']}, customer_name: {value['customer_name']}, "
                f"drink_name: {value['drink_name']}, quantity: {value['quantity']}, amount: {value['amount']}, "
                f"is_member: {value['is_member']}, ts_utc: {value['ts_utc']}}}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()