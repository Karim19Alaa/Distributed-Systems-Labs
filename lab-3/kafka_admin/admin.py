from kafka.admin import KafkaAdminClient, NewTopic
import os
import json
import time

def create_topics(admin_client, topics, num_partitions: int = 1, replication_factor: int = 1) -> None:
    topics_list = [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in topics]
    admin_client.create_topics(new_topics=topics_list, validate_only=False)

def create_admin(host: str):
    admin_client = None
    while admin_client is None:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=host)
        except Exception as e:
            print(e)
            print("Kafka is not ready yet.")
            time.sleep(1)
    return admin_client

if __name__ == "__main__":
    admin_client = create_admin(os.environ['KAFKA_HOST'])
    topics = json.loads(os.environ['TOPICS'])
    create_topics(admin_client, topics)
    admin_client.close()
