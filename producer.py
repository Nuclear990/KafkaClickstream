from confluent_kafka import Producer
import json, time, random
from datetime import datetime, timezone, timedelta

producer = Producer({"bootstrap.servers": "kafka:9092"})

def delivery_report(err, msg):
    if err:
        print(f"Failed: {err}")
    else:
        print(f"sent -> partition={msg.partition()} offset={msg.offset()}")


events = ["page_view", "click", "purchase1", "add_to_cart", "product_view", "add_to_cart", "add_to_cart", "add_to_cart"]
products = {1: 10, 2: 15, 3: 4, 4: 100, 5: 200, 6: 19, 7: 21, 8: 64, 9: 81, 10: 100}



print("Sending events... Ctrl+C to stop", flush=True)

try:
    i = 0
    while True:
        i += 1
        product_id = random.choice(list(products.keys()))
        product_price = products[product_id]
        user_id = random.randint(1, 100)
        event_type = random.choice(events)

        # Simulate ~10% late-arriving events
        if random.random() < 0.1:
            ts = datetime.now(timezone.utc) - timedelta(seconds=random.randint(5, 30))
        else:
            ts = datetime.now(timezone.utc)

        event = {
            "event_id": i,
            "user_id": user_id,
            "event_type": event_type,
            "product_id": product_id if event_type != "page_view" and event_type != "click" else None,
            "cost": product_price if event_type != "page_view" and event_type != "click" else None,
            "time_generated": ts.isoformat(),
            "time_received": datetime.now(timezone.utc).isoformat(),
        }

        producer.produce(
            topic="clickstream",
            key=str(user_id).encode(),
            value=json.dumps(event).encode(),
            on_delivery=delivery_report,
        )

        producer.poll(0)
        print(f"[{i}] user={user_id} event={event_type} product={product_id} produced at = {ts.isoformat()}", flush=True)
        time.sleep(random.randint(1, 10))

except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Done")