from confluent_kafka import AdminClient, NewTopic

admin = AdminClient({
    "bootstrap.servers": "kafka:9092"
})

def create_topic(name, partitions=3, replication=1):
    new_topic = NewTopic(
        topic=name,
        num_partitions=partitions,
        replication_factor=replication,
        config={
            #in ms
            "retention.ms": str(1*24*60*60*1000),
            "compression.type": "snappy"
        },
    )

    futures= admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created with {partitions} partitions")
        except Exception as e:
            print(f"Failed to create topic '{topic}' : {e}")

def list_topics():
    metadata = admin.list_topics(timeout=10)
    print(f"Topics in this cluster: ")

    for name, topic in metadata.topics.items():
        # Skip internal Kafka topics (they start with __)
        if not name.startswith("__"):
            print(f"    {name}: {len(topic.partitions)} partitions")