"""
    An ETL pipeline for processing museum exhibition data and loading it into
    a remote RDS database.
"""


import re
import sys
import logging
import argparse
import boto3
import pandas as pd
from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import sql
from extract import (get_matching_s3_files,
                     download_matching_files,
                     combine_csv_files)

CSV_FILE_PATTERN = re.compile(r"lmnh_hist_data_[0-9]\.csv")
CONFIG = dotenv_values()

# for if you want to connect to the local database instead of the remote RDS database

# conn = psycopg2.connect(
#     dbname="museum",
#     cursor_factory=RealDictCursor
# )

# conn = psycopg2.connect(
#     host=CONFIG["HOST"],
#     database=CONFIG["DATABASE"],
#     user=CONFIG["USERNAME"],
#     password=CONFIG["PASSWORD"],
#     cursor_factory=RealDictCursor
# )


def extract(bucket: str) -> pd.DataFrame:
    """Extracts the CSV data from S3 and combines it into a single DataFrame."""
    s3_client = boto3.client(
        's3', aws_access_key_id=CONFIG["ACCESS_KEY"], aws_secret_access_key=CONFIG["SECRET_KEY"])
    try:
        matching_files = get_matching_s3_files(
            bucket, s3_client, CSV_FILE_PATTERN)
    except RuntimeError as e:
        logging.critical("Error getting matching S3 files: %s", e)
        s3_client.close()
        print("Error getting matching S3 files")
        sys.exit()

    try:
        downloaded_files = download_matching_files(
            bucket, s3_client, matching_files, "./data")
    except RuntimeError as e:
        logging.critical("Error downloading files: %s", e)
        s3_client.close()
        print("Error downloading files")
        sys.exit()

    s3_client.close()

    combined_csv_data = combine_csv_files(downloaded_files)
    return combined_csv_data


def get_data_from_museum_table(table_name: str, connection) -> pd.DataFrame:
    """Fetches all data from the specified museum table and returns it as a DataFrame."""
    query = "SELECT * FROM {}"
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL(query).format(sql.Identifier(table_name)))
        data = cursor.fetchall()
        data = pd.DataFrame(data)
    return data


def transform(extracted_data: pd.DataFrame, exhibition_data: pd.DataFrame,
              request_data: pd.DataFrame,
              ratings_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Transforms the extracted data, splitting it into ratings
    and requests data and format them for insertion into the database"""

    # Drop rows where the 'val' column is NaN
    # This data is not useful as the button pressed cannot be identified
    size_before_transform = len(extracted_data.index)
    extracted_data = extracted_data.dropna(subset=['val'])
    size_after_dropna = len(extracted_data.index)
    logging.info(
        "Rows before dropping NaNs: %s, Rows after dropping NaNs: %s",
        size_before_transform, size_after_dropna)

    extracted_data = extracted_data.merge(
        exhibition_data, left_on="site", right_on="exhibition_id", how="inner")

    exhibition_ratings_df = extracted_data[extracted_data['val'] >= 0]

    # values below -1 are ignored as their button cannot be identified
    exhibition_requests_df = extracted_data[extracted_data['val'] == -1]

    exhibition_ratings_df = transform_exhibition_ratings_data(
        exhibition_ratings_df, ratings_data)

    exhibition_requests_df = transform_exhibition_requests_data(
        exhibition_requests_df, request_data)

    final_size = len(exhibition_ratings_df.index) + \
        len(exhibition_requests_df.index)
    total_rows_lost = size_before_transform - final_size
    log_rows_lost(total_rows_lost, size_before_transform)

    return exhibition_ratings_df, exhibition_requests_df


def log_rows_lost(total_rows_lost: int, size_before_transform: int) -> None:
    """Logs the number of rows lost during the transformation process"""
    if total_rows_lost > size_before_transform * 0.5:
        logging.critical(
            "Total rows lost is greater than 50%s of the original data: %s", "%", total_rows_lost)
    elif total_rows_lost > size_before_transform * 0.2:
        logging.error(
            "Total rows lost is greater than 20%s of the original data: %s", "%", total_rows_lost)
    elif total_rows_lost > 0:
        logging.warning(
            "Total rows lost: %s", total_rows_lost)
    else:
        logging.info(
            "No rows lost during the transformation process")


def transform_exhibition_ratings_data(exhibition_ratings_df: pd.DataFrame,
                                      ratings_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the exhibition ratings data by merging it with the ratings data,
    renaming columns, converting the time column to datetime, and selecting relevant columns.
    """
    size_before_merge = len(exhibition_ratings_df.index)

    exhibition_ratings_df = exhibition_ratings_df.merge(
        ratings_data, left_on="val", right_on="rating_value", how="inner")

    size_after_merge = len(exhibition_ratings_df.index)
    logging.info(
        "Rows before ratings merge: %s, Rows after ratings merge: %s",
        size_before_merge, size_after_merge)

    exhibition_ratings_df.rename(columns={'at': 'time_at'}, inplace=True)

    # Convert the 'time_at' column to datetime format
    exhibition_ratings_df['time_at'] = pd.to_datetime(
        # .dt.tz_localize('Europe/London')
        exhibition_ratings_df["time_at"], errors='coerce')

    # Select only the columns needed to be loaded into the database
    exhibition_ratings_df = exhibition_ratings_df[[
        'time_at', 'exhibition_id', 'rating_id']]

    size_before_dedupe = len(exhibition_ratings_df.index)
    exhibition_ratings_df = exhibition_ratings_df.drop_duplicates()
    logging.info(
        "Duplicate ratings removed: %s", size_before_dedupe - len(exhibition_ratings_df.index))

    return exhibition_ratings_df


def transform_exhibition_requests_data(exhibition_requests_df: pd.DataFrame,
                                       requests_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the exhibition requests data by merging it with the requests data,
    renaming columns, converting the time column to datetime, and selecting relevant columns.
    """
    size_before_merge = len(exhibition_requests_df.index)
    exhibition_requests_df = exhibition_requests_df.merge(
        requests_data, left_on="type", right_on="request_value", how="inner")

    size_after_merge = len(exhibition_requests_df.index)
    logging.info(
        "Rows before requests merge: %s, Rows after requests merge: %s",
        size_before_merge, size_after_merge)

    exhibition_requests_df.rename(columns={'at': 'time_at'}, inplace=True)

    # Convert the 'time_at' column to datetime format
    exhibition_requests_df['time_at'] = pd.to_datetime(
        # .dt.tz_localize('Europe/London')
        exhibition_requests_df["time_at"], errors='coerce')

    # Select only the columns needed to be loaded into the database
    exhibition_requests_df = exhibition_requests_df[[
        'time_at', 'exhibition_id', 'request_id']]

    size_before_dedupe = len(exhibition_requests_df.index)
    exhibition_requests_df = exhibition_requests_df.drop_duplicates()
    logging.info(
        "Duplicate requests removed: %s", size_before_dedupe - len(exhibition_requests_df.index))

    return exhibition_requests_df


def load(exhibition_ratings_df: pd.DataFrame, exhibition_requests_df: pd.DataFrame, conn) -> None:
    """Load transformed exhibition ratings and requests data into the database"""
    exhibition_ratings_list = list(
        exhibition_ratings_df.itertuples(index=False, name=None))
    exhibition_requests_list = list(
        exhibition_requests_df.itertuples(index=False, name=None))

    load_exhibition_ratings(exhibition_ratings_list, conn)
    load_exhibition_requests(exhibition_requests_list, conn)

    conn.commit()


def load_exhibition_ratings(exhibition_ratings_list: list[tuple], conn) -> None:
    """Load exhibition ratings into the database"""
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS exhibition_rating_staging")
        cursor.execute("""
            CREATE TABLE exhibition_rating_staging (
                time_at TIMESTAMP,
                exhibition_id INT,
                rating_id INT
            )
        """)
        execute_values(cursor,
                       """INSERT INTO exhibition_rating_staging
                            (time_at, exhibition_id, rating_id) VALUES %s""",
                       exhibition_ratings_list
                       )

        try:
            cursor.execute("""
                INSERT INTO exhibition_rating (time_at, exhibition_id, rating_id)
                    SELECT ers.time_at, ers.exhibition_id, ers.rating_id FROM exhibition_rating_staging ers
                        LEFT JOIN exhibition_rating er USING (time_at, exhibition_id, rating_id)
                            WHERE er.rating_id IS NULL""")
        except RuntimeError as e:
            logging.critical("Error loading exhibition ratings: %s", e)
            print("Error loading exhibition ratings, exiting program.")
            sys.exit()

        cursor.execute("DROP TABLE IF EXISTS exhibition_rating_staging")


def load_exhibition_requests(exhibition_requests_list: list[tuple], conn) -> None:
    """Load exhibition requests into the database"""
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS exhibition_request_staging")
        cursor.execute("""
            CREATE TABLE exhibition_request_staging (
                time_at TIMESTAMP,
                exhibition_id INT,
                request_id INT
            )
        """)
        execute_values(cursor,
                       """INSERT INTO exhibition_request_staging
                            (time_at, exhibition_id, request_id) VALUES %s""",
                       exhibition_requests_list
                       )

        try:
            cursor.execute("""
                INSERT INTO exhibition_request (time_at, exhibition_id, request_id)
                    SELECT ers.time_at, ers.exhibition_id, ers.request_id FROM exhibition_request_staging ers
                        LEFT JOIN exhibition_request er USING (time_at, exhibition_id, request_id)
                            WHERE er.request_id IS NULL""")
        except RuntimeError as e:
            logging.critical("Error loading exhibition requests: %s", e)
            print("Error loading exhibition requests, exiting program.")
            sys.exit()

        cursor.execute("DROP TABLE IF EXISTS exhibition_request_staging")


def run(bucket) -> None:
    """Run the ETL pipeline for the specified S3 bucket."""
    conn = psycopg2.connect(
        dbname="museum",
        cursor_factory=RealDictCursor
    )
    extracted_data = extract(bucket)

    print("Getting data from museum tables")
    exhibition_data = get_data_from_museum_table("exhibition", conn)
    request_data = get_data_from_museum_table("request", conn)
    ratings_data = get_data_from_museum_table("rating", conn)

    print("Transforming extracted data")
    exhibition_ratings_df, exhibition_requests_df = transform(
        extracted_data, exhibition_data, request_data, ratings_data)

    print("Loading transformed data into the database")
    load(exhibition_ratings_df, exhibition_requests_df, conn)
    conn.close()


if __name__ == "__main__":
    logging.basicConfig(filename="etl_pipeline.log",
                        filemode="w", level=logging.WARNING)

    parser = argparse.ArgumentParser(
        description="ETL pipeline for museum data")
    parser.add_argument("--log-level", default="WARNING",
                        help="Set the logging level, options are:  +\
                            DEBUG, INFO, WARNING, ERROR and CRITICAL")

    parser.add_argument("--bucket", help="Specify the S3 bucket name to search for museum data",
                        default="sigma-resources-museum")
    args = parser.parse_args()

    # Sets the logging level and S3 bucket based on command-line arguments
    log_level = args.log_level
    if log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        logging.getLogger().setLevel(log_level)
    else:
        logging.getLogger().setLevel("WARNING")
    s3_bucket = args.bucket

    print("Starting ETL pipeline")
    run(s3_bucket)
    # conn.close()
    print("ETL pipeline finished")
