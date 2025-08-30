from kafka import KafkaConsumer
import json
import os
import time
import requests
import logging
import uuid
from datetime import datetime
from collections import defaultdict

def setup_logger(name: str) -> logging.Logger:
    """Sets up a simple logger with the specified name."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def send_update(service: str, message: dict) -> None:
    """Sends update messages to the update server for display in the UI."""
    url = f"http://update_server:5000/update/{service}"
    try:
        requests.post(url, json=message)
    except Exception as e:
        logging.error(f"Failed to send update for {service}: {e}")

def update_action_data(action_counts: dict, action: str) -> int:
    action_counts[action] = action_counts[action] + 1
    return action_counts[action]

def get_analytics_summary(action_counts: dict) -> dict:
    """
    Generate a summary of analytics data.
    
    Args:
        action_counts: Dictionary tracking all action counts
        
    Returns:
        dict: Summary statistics about actions
    """
    total_actions = sum(action_counts.values())
    
    # Find the most common action
    most_common_action = None
    most_common_count = 0
    
    for action, count in action_counts.items():
        if count > most_common_count:
            most_common_action = action
            most_common_count = count
    
    return {
        "total_actions": total_actions,
        "unique_actions": len(action_counts),
        "most_common_action": most_common_action,
        "most_common_action_count": most_common_count
    }

def main() -> None:
    """Main function for the analytics service."""
    session_id = str(uuid.uuid4())[:8]
    logger = setup_logger('analytics_service')
    logger.info(f"Analytics Service Started with session ID {session_id}")
    
    # Get configuration from environment
    topics = json.loads(os.environ['SELECTED_TOPICS'])
    kafka_address = os.environ['EVENT_LISTENER_ADDRESS']
    
    logger.info(f"Configured to listen on topics: {', '.join(topics)}")
    
    consumer = None
    event_count = 0
    last_summary_time = time.time()
    summary_interval = 30  # Generate summary every 30 seconds
    
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=kafka_address,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            client_id=f"analytics_service_{session_id}",
            group_id=f"analytics_group_{session_id}"
        )
        logger.info(f"Successfully connected to Kafka for topics: {', '.join(topics)}")
        
        action_counts = defaultdict(int)
        
        for event in consumer:
            event_count += 1
            action = event.topic
            
            # Log the event
            logger.info(f"Event #{event_count}: Action {action} performed")
            
            # Update analytics
            action_count = update_action_data(action_counts, action)
            
            # Send update to UI
            send_update(
                "business", 
                {"message": f"Action: {action} has been performed {action_count} times."}
            )
            
            # Generate and log summary periodically
            current_time = time.time()
            if current_time - last_summary_time > summary_interval:
                summary = get_analytics_summary(action_counts)
                logger.info(f"Analytics Summary: {json.dumps(summary)}")
                last_summary_time = current_time
                
                # Send summary to UI
                timestamp = datetime.now().strftime("%H:%M:%S")
                summary_msg = (
                    f"[SUMMARY {timestamp}] Total Actions: {summary['total_actions']}, "
                    f"Unique Actions: {summary['unique_actions']}, "
                    f"Top Action: {summary['most_common_action']} ({summary['most_common_action_count']} times)"
                )
                send_update("business", {"message": summary_msg})
            
            time.sleep(0.1)
    except Exception as e:
        logger.error(f"Error in analytics service: {e}", exc_info=True)
    finally:
        logger.info(f"Shutting down analytics service, session {session_id}")
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed properly")

if __name__ == "__main__":
    main_logger = setup_logger('analytics_main')
    main_logger.info("Analytics application starting")
    
    try:
        main()
    except Exception as e:
        main_logger.critical(f"Fatal error in analytics application: {e}", exc_info=True)