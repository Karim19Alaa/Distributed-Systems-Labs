from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import os
from multiprocessing import Process
import requests
import logging
import uuid
import socket


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

def ad_listener(user_id: str, ad_host: str) -> None:
    session_id = str(uuid.uuid4())[:8]
    logger = setup_logger('ad_listener')
    logger.info(f"Ad Service Started for user {user_id} with session {session_id}")
    consumer = None
    
    try:
        logger.info(f"Connecting to Kafka broker at {ad_host} for topic targeted_ads_{user_id}")
        consumer = KafkaConsumer(
            f"targeted_ads_{user_id}",
            bootstrap_servers=ad_host,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            client_id=f"ad_listener_{user_id}_{session_id}",
            group_id=f"ad_group_{user_id}_{session_id}"
        )
        logger.info(f"Successfully connected to Kafka for ad topic targeted_ads_{user_id}")
        
        ad_count = 0
        for message in consumer:
            ad_count += 1
            ad_event = message.value
            logger.info(f"Received ad #{ad_count}: {ad_event['ad_content']} based on user activity at {ad_event['timestamp']}")
            send_update("ads", {"message": f"Ad Received: {ad_event['ad_content']} for User {user_id}"})
            time.sleep(0.1)
    except Exception as e:
        logger.error(f"Error in ad_listener for user {user_id}: {e}", exc_info=True)
    finally:
        logger.info(f"Shutting down ad listener for user {user_id}, session {session_id}")
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed properly")

def start_ad_listener() -> None:
    """
    Starts the ad listener process.
    """
    logger = setup_logger('start_ad_listener')
    user_id = os.environ["CLIENT_ID"]
    ad_host = os.environ['ADS_LISTENER_ADDRESS']
    logger.info(f"Starting ad listener process for user {user_id} connecting to {ad_host}")
    
    ad_process = Process(
        target=ad_listener,
        args=(user_id, ad_host)
    )
    ad_process.daemon = True  # Make the process terminate when the parent does
    ad_process.start()
    logger.info(f"Ad listener process started with PID {ad_process.pid}")

def event_producer(user_id: str, actions: list, event_host: str) -> None:
    """
    Produces events for the given user_id.
    """
    session_id = str(uuid.uuid4())[:8]  # Generate session ID for tracking
    logger = setup_logger('event_producer')
    logger.info(f"Event Producer Started for user {user_id} with session {session_id}")
    logger.info(f"Available actions: {', '.join(actions)}")
    
    producer = None
    event_count = 0
    
    try:
        logger.info(f"Connecting to Kafka broker at {event_host}")
        producer = KafkaProducer(
            bootstrap_servers=event_host,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id=f"event_producer_{user_id}_{session_id}"
        )
        logger.info(f"Successfully connected to Kafka for event production")
        
        while True:
            action = random.choice(actions)
            timestamp = int(time.time())
            event_count += 1
            
            event = {
                'user_id': user_id,
                'action': action,
                'timestamp': timestamp,
                'event_id': f"{user_id}_{timestamp}_{event_count}",
                'session_id': session_id
            }
            
            msg = f"User {user_id} performed action {action}"
            logger.info(msg)
            producer.send(event['action'], event)
            logger.debug(f"Event details: {json.dumps(event)}")
            
            # Send update to client view
            send_update("client", {
                "user_id": user_id,
                "action": action,
                "message": msg
            })
            
            delay = random.uniform(1, 5)
            logger.debug(f"Waiting {delay:.2f} seconds before next event")
            time.sleep(delay)
    except Exception as e:
        logger.error(f"Error in event_producer for user {user_id}: {e}", exc_info=True)
    finally:
        logger.info(f"Shutting down event producer for user {user_id}, session {session_id}")
        if producer:
            producer.close()
            logger.info("Kafka producer closed properly")

def start_event_producer() -> None:
    """
    Starts the event producer.
    """
    logger = setup_logger('start_event_producer')
    user_id = os.environ["CLIENT_ID"]
    actions = json.loads(os.environ['ACTIONS'])
    event_host = os.environ['EVENT_EMITTER_ADDRESS']
    
    logger.info(f"Client_{user_id} Started")
    logger.info(f"Environment settings: EVENT_EMITTER_ADDRESS={event_host}, ACTIONS={actions}")
    
    event_producer(user_id, actions, event_host)

if __name__ == "__main__":
    main_logger = setup_logger('client_main')
    main_logger.info(f"Client application starting - PID: {os.getpid()}")
    main_logger.info(f"Environment variables: {', '.join([f'{k}={v}' for k, v in os.environ.items() if k in ['CLIENT_ID', 'EVENT_EMITTER_ADDRESS', 'ADS_LISTENER_ADDRESS']])}")
    
    try:
        main_logger.info("Starting ad listener...")
        start_ad_listener()
        
        main_logger.info("Starting event producer...")
        start_event_producer()
    except Exception as e:
        main_logger.critical(f"Fatal error in client application: {e}", exc_info=True)