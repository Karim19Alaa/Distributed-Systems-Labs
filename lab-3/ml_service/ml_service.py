from kafka import KafkaConsumer, KafkaProducer
import json
import os

import logging


def main():
  logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
  logging.info(f"Machine Learning Service Started.")
  
  topics = json.loads(os.environ['SELECTED_TOPICS'])
  try:

    consumer = KafkaConsumer(*topics,
                            bootstrap_servers=os.environ['EVENT_LISTENER_ADDRESS'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    producer = KafkaProducer(bootstrap_servers=os.environ['ADS_EMITTER_ADDRESS'],
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for message in consumer:
      event = message.value
      # Mock ML model processing
      ad_content = f"Ad for action {event['action']}"
      ad_event = {
          'user_id': event['user_id'],
          'ad_content': ad_content,
          'timestamp': event['timestamp']
      }
      logging.info(f"Sending Ad to user {ad_event['user_id']} due to {event['action']}.")
      producer.send(f"targeted_ads_{ad_event['user_id']}", ad_event)
  finally:
    if consumer: consumer.close()
    if producer: producer.close()

if __name__ == "__main__":
    main()
