from kafka import KafkaConsumer, KafkaProducer
import json
import os


def main():
  topics = json.loads(os.environ['SELECTED_TOPICS'])

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
    try:
        producer.send(f"targeted_ads_{ad_event['user_id']}", ad_event)
    except:
      pass

if __name__ == "__main__":
    main()
