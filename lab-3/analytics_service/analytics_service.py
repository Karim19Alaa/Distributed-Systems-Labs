from kafka import KafkaConsumer
import json
import os

import logging

def update_user_data(user_id, users_action_count, action):
    user_action_count = users_action_count.get(user_id, {})
    user_action_count[action] = user_action_count.get(action, 0) + 1
    users_action_count[user_id] = user_action_count
    return user_action_count[action]

def main():
  logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
  logging.info(f"Analytics Service Started.")
  
  topics = json.loads(os.environ['SELECTED_TOPICS'])
  
  try:
    consumer = KafkaConsumer(*topics,
                                bootstrap_servers=os.environ['EVENT_LISTENER_ADDRESS'],
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    users_action_count = {}
    for event in consumer:
      value = event.value
      user_id = value['user_id']
      # Mock analysis: print user action count
      action_count = update_user_data(user_id, users_action_count, event.topic)
      logging.info(f"User {user_id} has performed {action_count} {event.topic}s.")
      
  finally:
    if consumer: consumer.close()


if __name__ == "__main__":
  main()
