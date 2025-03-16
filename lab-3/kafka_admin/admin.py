from kafka.admin import KafkaAdminClient, NewTopic
import os
import logging
import json
from typing import List

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up a logger with the specified name.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def create_topics(admin_client: KafkaAdminClient, topics: List[str], num_partitions: int = 1, replication_factor: int = 1) -> None:
    """
    Creates topics in Kafka.
    """
    topics_list = [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in topics]
    admin_client.create_topics(new_topics=topics_list, validate_only=False)

def create_admin(host: str) -> KafkaAdminClient:
    """
    Creates a Kafka admin client.
    """
    logger = setup_logger('kafka_admin')
    ready = False
    admin_client = None
    while not ready:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=host)
            ready = True
        except Exception as e:
            logger.debug(e)
            logger.debug("Kafka is not ready yet.")
    return admin_client

if __name__ == "__main__":
    logger = setup_logger('kafka_admin')
    
    admin_client = create_admin(os.environ['KAFKA_HOST'])
    topics = json.loads(os.environ['TOPICS'])
    create_topics(admin_client, topics)
    
    admin_client.close()