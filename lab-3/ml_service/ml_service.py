from kafka import KafkaConsumer, KafkaProducer
import json
import os
import time
import requests
import logging
import socket
import uuid
from datetime import datetime

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up a logger with the specified name and enhanced formatting.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    # Enhanced format with timestamp, hostname, and process ID
    formatter = logging.Formatter('%(asctime)s [%(hostname)s] [%(process)d] %(levelname)s:%(name)s:%(message)s')
    
    # Add hostname to log records
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.hostname = socket.gethostname()
        return record
    logging.setLogRecordFactory(record_factory)
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def send_update(service: str, message: dict) -> None:
    """
    Sends update messages to the update server for display in the UI.
    """
    url = f"http://update_server:5000/update/{service}"
    try:
        requests.post(url, json=message)
    except Exception as e:
        logging.error(f"Failed to send update for {service}: {e}")

def generate_ad_content(action: str, user_id: str) -> tuple:
    """
    Generate personalized ad content based on user action.
    This simulates an ML-based recommendation system.
    
    Args:
        action: The user action that triggered the ad
        user_id: The ID of the user
        
    Returns:
        tuple: (ad_content, ad_type)
    """
    # Simple mapping of actions to ad categories
    ad_categories = {
        "view": "Product recommendations",
        "click": "Special offers",
        "search": "Search results enhancement",
        "purchase": "Related products",
        "add_to_cart": "Cart completion deals"
    }
    
    # Get category or default
    category = ad_categories.get(action, "Personalized recommendations")
    
    # Add some randomization to make it look more realistic
    ad_adjectives = ["Exclusive", "Limited-time", "Special", "Personalized", "Premium"]
    ad_offers = ["discount", "deal", "offer", "selection", "recommendation"]
    
    import random
    adjective = random.choice(ad_adjectives)
    offer = random.choice(ad_offers)
    
    ad_content = f"{adjective} {category} {offer} for user {user_id}"
    
    # Determine ad type from content
    ad_type = "unknown"
    for keyword in ["discount", "deal", "offer", "recommendation", "selection"]:
        if keyword in ad_content.lower():
            ad_type = keyword
            break
    
    return ad_content, ad_type

def main() -> None:
    """
    Main function for the ML service.
    """
    session_id = str(uuid.uuid4())[:8]  # Generate session ID for tracking
    logger = setup_logger('ml_service')
    logger.info(f"Machine Learning Service Started with session ID {session_id}")
    
    # Get configuration from environment
    topics = json.loads(os.environ['SELECTED_TOPICS'])
    consumer_address = os.environ['EVENT_LISTENER_ADDRESS']
    producer_address = os.environ['ADS_EMITTER_ADDRESS']
    
    logger.info(f"Configured to listen on topics: {', '.join(topics)}")
    
    consumer = None
    producer = None
    event_count = 0
    ad_count = 0
    
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=consumer_address,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            client_id=f"ml_service_consumer_{session_id}",
            group_id=f"ml_service_group_{session_id}"
        )
        logger.info(f"Successfully connected to Kafka consumer for topics: {', '.join(topics)}")
        
        producer = KafkaProducer(
            bootstrap_servers=producer_address,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id=f"ml_service_producer_{session_id}"
        )
        logger.info(f"Successfully connected to Kafka producer")
        
        for message in consumer:
            event_count += 1
            event = message.value
            user_id = event['user_id']
            action = event['action']
            
            # Log the incoming event
            logger.info(f"Event #{event_count}: User {user_id} performed {action}")
            
            # Generate personalized ad content and ad type
            ad_content, ad_type = generate_ad_content(action, user_id)
            ad_count += 1
            
            # Create ad event
            ad_event = {
                'user_id': user_id,
                'ad_content': ad_content,
                'ad_type': ad_type,
                'timestamp': event['timestamp'],
                'trigger_action': action,
                'ad_id': f"ad_{user_id}_{ad_count}_{int(time.time())}",
                'ml_session': session_id
            }
            
            # Log and send the ad
            logger.info(f"Generated ad #{ad_count}: '{ad_content}' (type: {ad_type}) for User {user_id}")
            
            # Send updates to UI for ads view
            send_update("ads", {
                "ad_type": ad_type, 
                "message": f"Ad type {ad_type} sent to user {user_id} due to {action}."
            })
            
            # Send to appropriate topic
            producer.send(f"targeted_ads_{ad_event['user_id']}", ad_event)
            
            time.sleep(0.1)
    except Exception as e:
        logger.error(f"Error in machine learning service: {e}", exc_info=True)
    finally:
        logger.info(f"Shutting down ML service, session {session_id}")
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed properly")
        if producer:
            producer.close()
            logger.info("Kafka producer closed properly")

if __name__ == "__main__":
    main_logger = setup_logger('ml_main')
    main_logger.info(f"ML Service application starting - PID: {os.getpid()}")
    
    try:
        main()
    except Exception as e:
        main_logger.critical(f"Fatal error in ML service application: {e}", exc_info=True)