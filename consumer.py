from confluent_kafka import Consumer, KafkaError
import json

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "abandoned-carts-debug",
    "auto.offset.reset": "earliest"   # start from beginning for testing
})

consumer.subscribe(["abandoned_carts"])

print("Listening to abandoned_carts... (Ctrl+C to stop)", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print("Error:", msg.error())
                break

        # Decode key
        key = msg.key().decode() if msg.key() else None

        # Decode value (JSON)
        try:
            value = json.loads(msg.value().decode())
        except Exception:
            value = msg.value().decode()

        print(f"\n--- MESSAGE ---", flush=True)
        print(f"user_id (key): {key}", flush=True)
        print(f"value: {value}")
        print(f"partition: {msg.partition()}, offset: {msg.offset()}", flush=True)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer closed")