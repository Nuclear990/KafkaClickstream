from confluent_kafka import Producer
import json, time, random
from datetime import datetime, timezone

producer = Producer({
    "bootstrap.servers": "kafka:9092"
})

def delivery_report(err, msg):
    if err:
        print(f"Failed: {err}")
    else:
        print(f"sent -> partition = {msg.partition()}  offset = {msg.offset()}")

users = ["user1", "user2", "user3"]
pages = ["/home", "/products", "/cart"]
events = ["page_view", "click", "purchase"]

print("sending events... ctrl + C to stop")

try:
    i=0
    while True:
        i+=1
        event = {
            "user_id": random.choice(users),
            "event": random.choice(events),
            "page": random.choice(pages),
            "time": datetime.now(timezone.utc).isoformat()
        }

        producer.produce(
            topic="clickstream",
            key = event["user_id"].encode(),
            value = json.dumps(event).encode(),
            on_delivery=delivery_report
        )

        producer.poll(0)
        print(f"[{i}] {event['user_id']} -> {event['event']} on {event['page']}")
        time.sleep(1)

except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Done")