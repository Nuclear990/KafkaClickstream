from confluent_kafka import Consumer, KafkaError
import json

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "my-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["clickstream"])
        
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode())

        print(
            f"partition= {msg.partition()} offset= {msg.offset()} | ",
            f"{event['user_id']} -> {event['event_type']} on {event['product_id']}"
        )

        consumer.commit(asynchronous=False)


except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("consumer closed")