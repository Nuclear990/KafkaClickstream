from confluent_kafka import Producer
from datetime import datetime, timezone, timedelta
import random
import json
import time

# ======================================================
# CONFIG
# ======================================================

TOPIC = "clickstream"

NUM_USERS = 1000

KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "acks": "all",
    "enable.idempotence": True
}

producer = Producer(KAFKA_CONFIG)

# ======================================================
# USERS
# ======================================================

users = {}

for uid in range(1, NUM_USERS + 1):

    users[uid] = {

        "state": "idle",

        "cart": [],

        "last_product": None,

        "last_event_time": datetime.now(timezone.utc)
    }

# ======================================================
# PRODUCTS
# ======================================================

PRODUCT_IDS = list(range(1, 101))

# ======================================================
# EVENT COUNTER
# ======================================================

event_id = 0

# ======================================================
# DELIVERY REPORT
# ======================================================

def delivery_report(err, msg):

    if err:

        print(f"DELIVERY FAILED: {err}")

# ======================================================
# NEXT EVENT LOGIC
# ======================================================

def get_next_event(user):

    state = user["state"]

    # ==============================================
    # START OF JOURNEY
    # ==============================================

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

    # ==============================================
    # BROWSING
    # ==============================================

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

    # ==============================================
    # VIEWING PRODUCT
    # ==============================================

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

    # ==============================================
    # CART
    # ==============================================

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

# ======================================================
# BUILD EVENT
# ======================================================

def build_event(user_id, event_type):

    global event_id

    event_id += 1

    user = users[user_id]

    # ==============================================
    # PRODUCT
    # ==============================================

    product_id = None

    if event_type in [
        "product_view",
        "add_to_cart",
        "purchase"
    ]:

        if (
            user["last_product"]
            and random.random() < 0.7
        ):

            product_id = user["last_product"]

        else:

            product_id = random.choice(PRODUCT_IDS)

        user["last_product"] = product_id

    # ==============================================
    # EVENT TIME
    # ==============================================

    delay = random.randint(1, 15)

    user["last_event_time"] += timedelta(
        seconds=delay
    )

    generated_time = user[
        "last_event_time"
    ]

    # ==============================================
    # PROCESSING DELAY
    # ==============================================

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

# ======================================================
# USER STATE TRANSITIONS
# ======================================================

def mutate_user(user_id, event_type):

    user = users[user_id]

    # ==============================================
    # STATE TRANSITIONS
    # ==============================================

    if event_type in [
        "page_view",
        "search"
    ]:

        user["state"] = "browsing"

    elif event_type == "product_view":

        user["state"] = "viewing"

    elif event_type == "add_to_cart":

        user["state"] = "cart"

        user["cart"].append(
            user["last_product"]
        )

    elif event_type == "purchase":

        user["state"] = "idle"

        user["cart"] = []

    elif event_type == "exit":

        user["state"] = "idle"

        user["cart"] = []

# ======================================================
# SEND EVENT
# ======================================================

def send_event(event):

    producer.produce(
        topic=TOPIC,
        key=str(event["user_id"]).encode(),
        value=json.dumps(event).encode(),
        on_delivery=delivery_report
    )

    print(event)

    # ==============================================
    # DUPLICATES
    # ==============================================

    if random.random() < 0.02:

        producer.produce(
            topic=TOPIC,
            key=str(event["user_id"]).encode(),
            value=json.dumps(event).encode(),
            on_delivery=delivery_report
        )

    producer.poll(0)

# ======================================================
# MAIN LOOP
# ======================================================

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
                    event_type
                )

                continue

            event = build_event(
                user_id,
                event_type
            )

            send_event(event)

            mutate_user(
                user_id,
                event_type
            )

        producer.flush()

        time.sleep(1)

except KeyboardInterrupt:

    print("Stopping producer...")

finally:

    producer.flush()

    print("Producer closed.")