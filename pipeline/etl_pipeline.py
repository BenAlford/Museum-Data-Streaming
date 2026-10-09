"""ETL pipeline for processing Kafka messages."""
import argparse
import json
import sys
import logging
from multiprocessing import Pool
from os import environ
from typing import List
import psycopg2
from psycopg2.extras import RealDictCursor

import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import Consumer

from read_kafka import extract, validate_message  # noqa: E402
from pipeline import (transform,                         # noqa: E402
                      get_data_from_museum_table,
                      load)

load_dotenv()

conn = psycopg2.connect(
    host=environ["DB_HOST"],
    database=environ["DB_NAME"],
    user=environ["DB_USERNAME"],
    password=environ["DB_PASSWORD"],
    cursor_factory=RealDictCursor
)

# Get the required master data from the museum tables
EXHIBITION_DATA = get_data_from_museum_table("exhibition", conn)
EXHIBITION_RATINGS_DATA = get_data_from_museum_table("rating", conn)
EXHIBITION_REQUESTS_DATA = get_data_from_museum_table("request", conn)


def run(batch_size: int, max_wait_time: int):
    """Run the ETL pipeline by consuming Kafka messages and processing them."""
    logging.info("Starting Kafka consumer")
    kafka_consumer = Consumer({
        "bootstrap.servers": environ["BOOTSTRAP_SERVERS"],
        'security.protocol': environ["SECURITY_PROTOCOL"],
        'sasl.mechanism': environ["SASL_MECHANISM"],
        'sasl.username': environ["USERNAME"],
        'sasl.password': environ["PASSWORD"],
        'group.id': 'c26-ben-group-id-new3',
        'auto.offset.reset': 'earliest'
    })
    logging.info("Kafka consumer initialized")

    extract(kafka_consumer, transform_data,
            batch_size, max_wait_time)


def get_valid_kafka_messages(rows: List[dict]) -> List[dict]:
    """
        Validates and decodes a batch of Kafka messages
        removing invalid messages.
    """
    decoded_rows = []
    with Pool() as pool:
        for i, result in enumerate(pool.map(validate_message, rows)):
            if result:
                row = json.loads(rows[i].value().decode('utf-8'))
                row["site"] = int(row["site"])
                decoded_rows.append(row)
    return decoded_rows


def transform_data(rows: List[dict]):
    """Transform the incoming row and load it into the appropriate table."""
    logging.info("Validating incoming rows")
    rows_start = len(rows)
    decoded_rows = get_valid_kafka_messages(rows)
    rows_lost = rows_start - len(decoded_rows)
    logging.info("Rows lost during validation: %s", rows_lost)

    new_data = pd.DataFrame(decoded_rows, columns=[
                            "at", "site", "val", "type"])

    if new_data.empty:
        logging.info("No new data to process")
        return

    ratings_data, requests_data = transform(new_data, EXHIBITION_DATA, EXHIBITION_REQUESTS_DATA,
                                            EXHIBITION_RATINGS_DATA)
    logging.info("Loading transformed data into the database")
    load(ratings_data, requests_data, conn)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, filename="etl_pipeline.log",
                        encoding="utf-8", filemode="a")

    parser = argparse.ArgumentParser(
        description="ETL pipeline for museum data")
    parser.add_argument("--log-level", default="INFO",
                        help="Set the logging level, options are:  +\
                                DEBUG, INFO, WARNING, ERROR and CRITICAL")

    parser.add_argument("--max-batch-size", help="The size threshold for a batch +\
                            to be sent off for processing",
                        default=100, type=int)
    parser.add_argument("--max-wait-time", help="The maximum wait time before a batch +\
                            is sent off for processing",
                        default=5, type=int)

    args = parser.parse_args()

    # Sets parameters based on command-line arguments
    log_level = args.log_level
    if log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        logging.getLogger().setLevel(log_level)
    else:
        logging.getLogger().setLevel("WARNING")

    batch_size_arg = int(args.max_batch_size)
    max_wait_time_arg = int(args.max_wait_time)

    if batch_size_arg <= 0:
        logging.error("Invalid batch size: %s", batch_size_arg)
        sys.exit(1)

    if max_wait_time_arg <= 0:
        logging.error("Invalid max wait time: %s", max_wait_time_arg)
        sys.exit(1)

    run(batch_size_arg, max_wait_time_arg)
    conn.close()
