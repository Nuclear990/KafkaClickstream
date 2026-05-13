from confluent_kafka import Producer
from datetime import datetime, timezone, timedelta
import random
import json
import time

# Config

TOPIC = "clickstream"

NUM_USERS = 1000

KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "acks": "all",
    "enable.idempotence": True
}

producer = Producer(KAFKA_CONFIG)

# Users

users = {}

for uid in range(1, NUM_USERS + 1):

    users[uid] = {

        "state": "idle",

        "cart": set(),

        "last_product": None,

        "last_event_time": datetime.now(timezone.utc)
    }

# Products

PRODUCT_IDS = list(range(1, 101))

# Event Counter

event_id = 0

# Delivery Report

def delivery_report(err, msg):

    if err:

        print(f"DELIVERY FAILED: {err}")

# Next Event Logic

def get_next_event(user):

    state = user["state"]

    if state == "idle":

        return random.choices(
            [
                "page_view",
                "search",
                "product_view"
            ],
            weights=[4, 3, 3],
            k=1
        )[0]

    if state == "browsing":

        return random.choices(
            [
                "search",
                "product_view",
                "page_view",
                "exit"
            ],
            weights=[3, 5, 2, 1],
            k=1
        )[0]

    if state == "viewing":

        return random.choices(
            [
                "product_view",
                "add_to_cart",
                "search",
                "exit"
            ],
            weights=[3, 5, 1, 1],
            k=1
        )[0]

    if state == "cart":

        return random.choices(
            [
                "add_to_cart",
                "purchase",
                "product_view",
                "exit"
            ],
            weights=[2, 5, 1, 2],
            k=1
        )[0]

# Build Event

def build_event(user_id, event_type):

    global event_id

    event_id += 1

    user = users[user_id]

    product_id = None

    if event_type in [
        "product_view",
        "add_to_cart",
        "purchase"
    ]:

        # purchases must come from cart
        if (
            event_type == "purchase"
            and len(user["cart"]) > 0
        ):

            product_id = random.choice(
                list(user["cart"])
            )

        elif (
            user["last_product"]
            and random.random() < 0.7
        ):

            product_id = user["last_product"]

        else:

            product_id = random.choice(PRODUCT_IDS)

        user["last_product"] = product_id

    delay = random.randint(1, 15)

    user["last_event_time"] += timedelta(
        seconds=delay
    )

    generated_time = user[
        "last_event_time"
    ]

    receive_delay = random.randint(0, 5)

    # late events
    if random.random() < 0.1:

        receive_delay += random.randint(
            30,
            90
        )

    received_time = (
        generated_time
        + timedelta(seconds=receive_delay)
    )

    return {

        "event_id": event_id,

        "user_id": user_id,

        "event_type": event_type,

        "product_id": product_id,

        "time_generated":
            generated_time.isoformat(),

        "time_received":
            received_time.isoformat()
    }

# User State Transitions

def mutate_user(user_id, event):

    user = users[user_id]

    event_type = event["event_type"]

    if event_type in [
        "page_view",
        "search"
    ]:

        user["state"] = "browsing"

    elif event_type == "product_view":

        user["state"] = "viewing"

    elif event_type == "add_to_cart":

        user["state"] = "cart"

        user["cart"].add(
            event["product_id"]
        )

    elif event_type == "purchase":

        purchased_product = event["product_id"]

        user["cart"].discard(
            purchased_product
        )

        if len(user["cart"]) == 0:

            user["state"] = "idle"

    elif event_type == "exit":

        user["state"] = "idle"

        user["cart"] = set()

# Send Event

def send_event(event):

    producer.produce(
        topic=TOPIC,
        key=str(event["user_id"]).encode(),
        value=json.dumps(event).encode(),
        on_delivery=delivery_report
    )

    print(event)

    # duplicates
    if random.random() < 0.02:

        producer.produce(
            topic=TOPIC,
            key=str(event["user_id"]).encode(),
            value=json.dumps(event).encode(),
            on_delivery=delivery_report
        )

    producer.poll(0)

# Main Loop

print("Starting producer...")

try:

    while True:

        active_users = random.sample(
            list(users.keys()),
            random.randint(20, 100)
        )

        for user_id in active_users:

            user = users[user_id]

            event_type = get_next_event(user)

            # exits are silent
            if event_type == "exit":

                mutate_user(
                    user_id,
                    {
                        "event_type": "exit"
                    }
                )

                continue

            event = build_event(
                user_id,
                event_type
            )

            send_event(event)

            mutate_user(
                user_id,
                event
            )

        producer.flush()

        time.sleep(1)

except KeyboardInterrupt:

    print("Stopping producer...")

finally:

    producer.flush()

    print("Producer closed.")