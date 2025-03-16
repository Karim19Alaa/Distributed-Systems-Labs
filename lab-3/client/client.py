from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import os
from threading import Thread


def ad_listner(consumer: KafkaConsumer):
  for message in consumer:
    ad_event = message.value
    print(f"Ad: {ad_event['ad_content']}")


def event_producer(producer: KafkaProducer, user_id):
  actions = os.environ['ACTIONS']

  while True:
      event = {
          'user_id': user_id,
          'action': random.choice(actions),
          'timestamp': int(time.time())
      }
      try:
        producer.send(event['action'], event)
      except:
        pass
      time.sleep(random.uniform(1, 5))

def main():
  consumer = KafkaConsumer(f"targeted_ads_{user_id}",
                          bootstrap_servers=os.environ['ADS_LISTENER_ADDRESS'],
                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
  ad_thread = Thread(target=ad_listner, args=(consumer))
  ad_thread.start()

  producer = KafkaProducer(bootstrap_servers=os.environ['EVENT_EMITTER_ADDRESS'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
  user_id = os.environ["CLIENT_ID"]
  event_producer(producer, user_id)



if __name__ == "__main":
  main()