from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import os
from multiprocessing import Process
import logging


def ad_listener(user_id, ad_host):
  logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
  logging.info("Ad Service Started.")
  try:
    consumer = KafkaConsumer(f"targeted_ads_{user_id}",
                          bootstrap_servers=ad_host,
                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    for message in consumer:
      ad_event = message.value
      logging.info(f"{ad_event['ad_content']}")
  finally:
    if consumer: consumer.close()
    
def start_ad_listener():
  ad_process = Process(target=ad_listener, args=(os.environ["CLIENT_ID"], os.environ['ADS_LISTENER_ADDRESS']))
  ad_process.start()
    

def event_producer(user_id, actions, event_host):
  try:
    producer = KafkaProducer(bootstrap_servers=event_host,
                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
      event = {
        'user_id': user_id,
        'action': random.choice(actions),
        'timestamp': int(time.time())
      }
      producer.send(event['action'], event)
      logging.info(f"{event['action']} event sent.")
      time.sleep(random.uniform(1, 5))
  finally:
    if producer: producer.close()

def start_event_producer():
  logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
  user_id = os.environ["CLIENT_ID"]
  
  logging.info(f"Client_{user_id} Started.")
  
  event_producer(user_id, json.loads(os.environ['ACTIONS']), os.environ['EVENT_EMITTER_ADDRESS'])
    


if __name__ == "__main__":
  start_ad_listener()
  start_event_producer()
    