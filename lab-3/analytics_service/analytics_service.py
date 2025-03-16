from kafka import KafkaConsumer
import json
import os
import logging
from typing import Dict

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up a logger with the specified name.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def update_user_data(user_id: str, users_action_count: Dict[str, Dict[str, int]], action: str) -> int:
    """
    Updates the action count for a given user and action.
    """
    user_action_count = users_action_count.get(user_id, {})
    user_action_count[action] = user_action_count.get(action, 0) + 1
    users_action_count[user_id] = user_action_count
    return user_action_count[action]

def main() -> None:
    """
    Main function to start the analytics service.
    """
    logger = setup_logger('analytics_service')
    logger.info("Analytics Service Started.")
    
    topics = json.loads(os.environ['SELECTED_TOPICS'])
    
    consumer = None
    try:
        consumer = KafkaConsumer(*topics,
                                 bootstrap_servers=os.environ['EVENT_LISTENER_ADDRESS'],
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

        users_action_count: Dict[str, Dict[str, int]] = {}
        for event in consumer:
            value = event.value
            user_id = value['user_id']
            # Mock analysis: print user action count
            action_count = update_user_data(user_id, users_action_count, event.topic)
            logger.info(f"User {user_id} has performed {action_count} {event.topic}s.")
    except Exception as e:
        logger.error(f"Error in analytics service: {e}")
    finally:
        if consumer:
            consumer.close()

if __name__ == "__main__":
    main()