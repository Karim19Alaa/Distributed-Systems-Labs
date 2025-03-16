from kafka.admin import KafkaAdminClient, NewTopic
import os
import logging
import json

def create_topics(admin_client, topics, num_partitions=1, replication_factor=1):
    topics_list = []
    for topic in topics:
        topics_list.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor))
        
    admin_client.create_topics(new_topics=topics_list, validate_only=False)
    
def create_admin(host):
    ready = False
    while not ready:
      try:
        admin_client = KafkaAdminClient(bootstrap_servers=host)
        ready = True
      except Exception as e:
        logging.debug(e)
        logging.debug("Kafka is not ready yet.")
      
    return admin_client

if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.DEBUG)
    
    admin_client = create_admin(os.environ['KAFKA_HOST'])
    topics = json.loads(os.environ['TOPICS'])
    create_topics(admin_client ,topics)
        
    admin_client.close()
    
