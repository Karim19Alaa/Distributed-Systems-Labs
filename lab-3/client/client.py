from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import os
from multiprocessing import Process
import logging
from typing import List


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


def ad_listener(user_id: str, ad_host: str) -> None:
    """
    Listens for targeted ads for the given user_id from the specified ad_host.
    """
    logger = setup_logger('ad_listener')
    logger.info("Ad Service Started.")
    consumer = None
    try:
        consumer = KafkaConsumer(f"targeted_ads_{user_id}",
                                 bootstrap_servers=ad_host,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        for message in consumer:
            ad_event = message.value
            logger.info(f"{ad_event['ad_content']}")
    except Exception as e:
        logger.error(f"Error in ad_listener: {e}")
    finally:
        if consumer:
            consumer.close()


def start_ad_listener() -> None:
    """
    Starts the ad listener process.
    """
    ad_process = Process(target=ad_listener, args=(os.environ["CLIENT_ID"], os.environ['ADS_LISTENER_ADDRESS']))
    ad_process.start()


def event_producer(user_id: str, actions: List[str], event_host: str) -> None:
    """
    Produces events for the given user_id to the specified event_host.
    """
    logger = setup_logger('event_producer')
    producer = None
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
            logger.info(f"{event['action']} event sent.")
            time.sleep(random.uniform(1, 5))
    except Exception as e:
        logger.error(f"Error in event_producer: {e}")
    finally:
        if producer:
            producer.close()


def start_event_producer() -> None:
    """
    Starts the event producer.
    """
    logger = setup_logger('start_event_producer')
    user_id = os.environ["CLIENT_ID"]
    logger.info(f"Client_{user_id} Started.")
    event_producer(user_id, json.loads(os.environ['ACTIONS']), os.environ['EVENT_EMITTER_ADDRESS'])


if __name__ == "__main__":
    start_ad_listener()
    start_event_producer()