from kafka import KafkaConsumer
import json
import os


def main():
  topics = json.loads(os.environ['SELECTED_TOPICS'])

  consumer = KafkaConsumer(*topics,
                          bootstrap_servers=os.environ['EVENT_LISTENER_ADDRESS'],
                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))

  user_action_count = {}


  for message in consumer:
      event = message.value
      user_id = event['user_id']
      if user_id not in user_action_count:
          user_action_count[user_id] = 0
      user_action_count[user_id] += 1
      # Mock analysis: print user action count
      print(f"User {user_id} has performed {user_action_count[user_id]} actions.")


if __name__ == "__main__":
  main()