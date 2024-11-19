from confluent_kafka.admin import AdminClient, NewTopic
from kafka.settings import KAFKA_BOOTSTRAP_SERVERS

admin_client = AdminClient({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
})

def create_topic(topic_name):
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic_name not in topic_metadata.topics:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic {topic} created successfully")
                return True
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
                return False
    else:
        print(f"Topic {topic_name} already exists")
        return True
