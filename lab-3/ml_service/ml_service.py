from kafka import KafkaConsumer, KafkaProducer
import json
import os
import logging
from typing import List, Dict, Any


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


def main() -> None:
    """
    Main function to start the machine learning service.
    """
    logger = setup_logger('ml_service')
    logger.info("Machine Learning Service Started.")
    
    topics: List[str] = json.loads(os.environ['SELECTED_TOPICS'])
    consumer: KafkaConsumer = None
    producer: KafkaProducer = None
    
    try:
        consumer = KafkaConsumer(*topics,
                                 bootstrap_servers=os.environ['EVENT_LISTENER_ADDRESS'],
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

        producer = KafkaProducer(bootstrap_servers=os.environ['ADS_EMITTER_ADDRESS'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        for message in consumer:
            event: Dict[str, Any] = message.value
            # Mock ML model processing
            ad_content: str = f"Ad for action {event['action']}"
            ad_event: Dict[str, Any] = {
                'user_id': event['user_id'],
                'ad_content': ad_content,
                'timestamp': event['timestamp']
            }
            logger.info(f"Sending Ad to user {ad_event['user_id']} due to {event['action']}.")
            producer.send(f"targeted_ads_{ad_event['user_id']}", ad_event)
    except Exception as e:
        logger.error(f"Error in machine learning service: {e}")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()


if __name__ == "__main__":
    main()