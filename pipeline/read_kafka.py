"""Reads and validates messages from a Kafka topic."""
from os import environ
from threading import Thread, Lock, Event
import json
from datetime import datetime, time as datetime_time, timedelta
import logging
import time

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, Message


def consume_loop(consumer: Consumer, topics, mutex: Lock, batch: list, stop_event: Event, transform_fn=None):
    """Consume messages from Kafka topics until the stop event is set."""
    logging.info("Starting consume loop for topics: %s", topics)

    try:
        consumer.subscribe(topics)

        while not stop_event.is_set():
            msg: Message = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                logging.error("Kafka error: %s", msg.error())
                continue
            else:
                with mutex:
                    batch.append(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        logging.info("Consumer closed. Exiting.")


def validate_datetime(dt: datetime) -> bool:
    """Validate that the datetime is not in the future and within the museum's opening times"""
    if dt > datetime.now(dt.tzinfo) + timedelta(minutes=5):
        logging.info("This message's at value is in the future")
        return False

    time_part = dt.time()
    if time_part < datetime_time(8, 45, 0) or time_part > datetime_time(18, 15, 0):
        logging.info("This message's at value is outside opening hours")
        return False

    return True


def validate_val_and_type(val, type_field) -> bool:
    """Validate the val and type fields in the Kafka message."""
    # Checks if val falls within the expected range (-1 for requests, 0-4 for ratings)
    if isinstance(val, bool) or not isinstance(val, int) or val < -1 or val > 4:
        logging.info("Invalid val in message: %s", val)
        return False

    # If val is -1, it represents a request and requires a type field validation
    # 0 is for assistance, 1 is for emergency
    if val == -1:
        if isinstance(type_field, bool):
            logging.info("Invalid type field for val -1: %s", type_field)
            return False

        if type_field not in [0, 1]:
            logging.info(
                "Invalid type field for val -1: %s", type_field)
            return False

    return True


def validate_site(site) -> bool:
    """Validate the site field in the Kafka message."""
    if isinstance(site, bool):
        logging.info("Invalid site in message: %s", site)
        return False

    if isinstance(site, str):
        if site.isnumeric() and site.isascii():
            site = int(site)
        else:
            logging.info("Unable to convert site to int: %s", site)
            return False

    if not isinstance(site, int):
        logging.info("Invalid site type: %s, %s", site, type(site))
        return False

    if site < 0 or site > 5:
        logging.info("Site out of range: %s", site)
        return False

    return True


def validate_message(msg: Message) -> bool:
    """Validate the Kafka message structure."""
    if msg is None:
        logging.info("Message is None")
        return False
    try:
        data = json.loads(msg.value().decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Failed to decode message: %s", e)
        return False

    if not isinstance(data, dict):
        logging.info("Message is not a dictionary")
        return False

    if "at" not in data or "site" not in data or "val" not in data:
        logging.info("Message is missing required fields")
        return False

    try:
        dt = datetime.fromisoformat(data["at"])
    except (ValueError, TypeError) as e:
        logging.error("Failed to parse datetime: %s", e)
        return False

    if not validate_datetime(dt):
        logging.info("Invalid datetime in message")
        return False

    val = data.get("val")
    type_field = data.get("type", None)

    if not validate_val_and_type(val, type_field):
        return False

    site = data.get("site")

    if not validate_site(site):
        return False

    return True


def batch_manager(mutex, batch_size, timeout, batch, stop_event, transform_func):
    """Periodically sends batches of messages to be processed by the transform function."""
    start_time = time.time()
    while not stop_event.is_set():
        with mutex:
            if len(batch) >= batch_size or (time.time() - start_time) >= timeout:
                # Process the batch
                if batch:
                    Thread(target=transform_func, args=(
                        batch.copy(),)).start()
                    batch.clear()
                start_time = time.time()
        time.sleep(1)


def extract(consumer, transform_func, batch_size, max_wait_time):
    """Extract messages from Kafka and process them in batches using the transform function."""
    mutex = Lock()
    stop_event = Event()
    batch = []
    cl = Thread(target=consume_loop, args=(
        consumer, ["lmnh"], mutex, batch, stop_event))
    cl.start()

    bm = Thread(target=batch_manager, args=(
        mutex, batch_size, max_wait_time, batch, stop_event, transform_func))
    bm.start()

    while True:
        user_input = input("Type quit to stop...\n")
        if user_input.strip().lower() == "quit":
            break

    stop_event.set()
    cl.join()
    bm.join()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, filename="read_kafka.log",
                        encoding="utf-8", filemode="a")

    load_dotenv()

    logging.info("Starting Kafka consumer")
    kafka_consumer = Consumer({
        "bootstrap.servers": environ["BOOTSTRAP_SERVERS"],
        'security.protocol': environ["SECURITY_PROTOCOL"],
        'sasl.mechanism': environ["SASL_MECHANISM"],
        'sasl.username': environ["USERNAME"],
        'sasl.password': environ["PASSWORD"],
        'group.id': 'c26-ben-group-id2',
        'auto.offset.reset': 'earliest'
    })
    logging.info("Kafka consumer initialized")

    extract(kafka_consumer, transform_func=None,
            batch_size=50, max_wait_time=5)
