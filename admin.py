from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({
    "bootstrap.servers": "kafka:9092"
})

def create_topic(name, partitions=3, replication=1, config=None):
    if config is None:
        config = {
            "retention.ms": str(1 * 24 * 60 * 60 * 1000),
            "compression.type": "snappy"
        }

    new_topic = NewTopic(
        topic=name,
        num_partitions=partitions,
        replication_factor=replication,
        config=config,
    )

    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created with {partitions} partitions")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")


create_topic("clickstream")

create_topic(
    "abandoned_carts",
    partitions=3,
    replication=1,
    config={
        "cleanup.policy": "compact,delete",
        "retention.ms": str(5 * 60 * 1000),      # 5 min TTL
        "delete.retention.ms": str(60 * 1000),   # tombstone cleanup
        "segment.ms": "60000",                   # faster segment roll
        "min.cleanable.dirty.ratio": "0.01",     # aggressive compaction
        "min.compaction.lag.ms": "0"
    }
)