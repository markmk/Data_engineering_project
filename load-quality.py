"""
Module: load-quality.py

This script processes hospital quality data from a CSV file and loads it into a PostgreSQL database.
It verifies the existence of hospitals, adds missing location and hospital records, and inserts quality ratings in batches.

Usage:
    python load-quality.py <rating_date> <quality-data-file.csv>
"""

import sys
import csv
import psycopg
from datetime import datetime
import credentials  # Import credentials

# Database connection
DB_CONFIG = {
    'host': credentials.DB_HOST,
    'dbname': credentials.DB_NAME,
    'user': credentials.DB_USER,
    'password': credentials.DB_PASSWORD
}

BATCH_SIZE = 1000  # Number of rows to process per batch


def main():
    """
    Main function to parse arguments, process the input CSV file, and load data into the database.

    Validates the rating date format and file existence, establishes a database connection,
    and iteratively processes each row in the CSV file.
    """
    # Checks correct number of arguments
    if len(sys.argv) != 3:
        print("Usage: python load-quality.py <rating_date> <quality-data-file.csv>")
        sys.exit(1)

    # Access arguments
    rating_date_str = sys.argv[1]
    csv_file = sys.argv[2]

    # Parse rating date
    try:
        rating_date = datetime.strptime(rating_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    # Establish database connection
    conn = psycopg.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            location_batch = []
            hospital_batch = []
            quality_batch = []

            for row_count, row in enumerate(reader, start=1):
                process_row(location_batch, hospital_batch, quality_batch, row, rating_date)

                # Insert batches when reaching the batch size
                if row_count % BATCH_SIZE == 0:
                    insert_batches(cur, location_batch, hospital_batch, quality_batch)
                    print(f"Processed {row_count} rows...")
                    # Clear batches after insertion
                    location_batch.clear()
                    hospital_batch.clear()
                    quality_batch.clear()

            # Insert remaining records
            if location_batch or hospital_batch or quality_batch:
                insert_batches(cur, location_batch, hospital_batch, quality_batch)
                print(f"Processed {row_count} rows...")

        # Commit transaction
        conn.commit()
        print("\nData loaded successfully.")

    except FileNotFoundError:
        print(f"File not found: {csv_file}")
        sys.exit(1)
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")


def process_row(location_batch, hospital_batch, quality_batch, row, rating_date):
    """
    Processes a single row of the CSV file and adds it to the appropriate batch.

    Args:
        location_batch (list): Batch for location records.
        hospital_batch (list): Batch for hospital records.
        quality_batch (list): Batch for quality ratings.
        row (dict): A dictionary containing a single row of CSV data.
        rating_date (datetime.date): The date of the rating.
    """
    facility_id = row['Facility ID']
    hospital_name = row['Facility Name']
    city = row['City']
    state = row['State']
    zip_code = row['ZIP Code']
    ownership = row['Hospital Ownership']
    emergency_services = parse_boolean(row['Emergency Services'])
    hospital_type = row['Hospital Type']
    quality_rating = parse_quality_rating(row['Hospital overall rating'])

    # Add to location batch
    location_batch.append((city, state, zip_code))

    # Add to hospital batch
    hospital_batch.append((facility_id, hospital_name, city, state, zip_code))

    # Add to quality batch
    quality_batch.append((
        facility_id, quality_rating, rating_date, ownership, hospital_type, emergency_services
    ))


def insert_batches(cursor, location_batch, hospital_batch, quality_batch):
    """
    Inserts batched records into the database with improved location handling.
    """
    cursor.executemany("""
        INSERT INTO location (city, state, zip_code)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
    """, location_batch)

    # Updated hospital insertion to handle multiple matching locations
    cursor.executemany("""
        INSERT INTO hospital (hospital_pk, hospital_name, location_id)
        VALUES (%s, %s, (
            SELECT id FROM location 
            WHERE city = %s AND state = %s AND zip_code = %s 
            ORDER BY id LIMIT 1
        ))
        ON CONFLICT DO NOTHING
    """, hospital_batch)

    cursor.executemany("""
        INSERT INTO hospital_quality (
            facility_id, quality_rating, rating_date, ownership, hospital_type, provides_emergency_services
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, quality_batch)


def parse_quality_rating(value):
    """
    Parses and validates the hospital quality rating.

    Args:
        value (str): The quality rating as a string from the CSV.

    Returns:
        int or None: Parsed rating if valid, otherwise None.
    """
    if not value or value.strip() == 'Not Available':
        return None
    rating = int(value) if value.isdigit() else None
    # Ensure rating is between 1 and 5 per the CHECK constraint
    if rating is not None and (rating < 1 or rating > 5):
        return None
    return rating


def parse_boolean(value):
    """
    Parses a boolean value from a string.

    Args:
        value (str): A string representing a boolean value ('yes' or other).

    Returns:
        bool: True if the value is 'yes' (case-insensitive), otherwise False.
    """
    if not value:
        return False
    return value.strip().lower() == 'yes'


if __name__ == "__main__":
    main()
