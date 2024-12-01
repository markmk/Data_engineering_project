"""
Module: load-quality.py

This script processes hospital quality data from a CSV file and loads it into a PostgreSQL database.
It verifies the existence of hospitals, adds missing location and hospital records, and inserts quality ratings.

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

def main():
    """
    Main function to parse arguments, process the input CSV file, and load data into the database.

    Validates the rating date format and file existence, establishes a database connection,
    and iteratively processes each row in the CSV file.
    """
    # Checks correct number of arguments
    if len(sys.argv) != 3:
        print("Usage: python load-quality.py <rating_date><quality-data-file.csv>")
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

    # Open CSV file and process each row
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                try:
                    process_and_insert_row(cur, row, rating_date)  # Corrected to pass only three arguments
                    row_count += 1
                    # Added counter to print progress every 100 rows
                    if row_count % 100 == 0:
                        print(f"Processed {row_count} rows...")
                except Exception:
                    print(f"\nError processing row {row_count + 1}:")
                    print("Row data:", row)
                    raise
        # Commit transaction
        conn.commit()
        print(f"\nData loaded successfully. Processed {row_count} rows.")

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


def process_and_insert_row(cursor, row, rating_date):
    """
    Processes a single row of the CSV file and inserts it into the database.

    Args:
        cursor (psycopg.Cursor): Database cursor for executing queries.
        row (dict): A dictionary containing a single row of CSV data.
        rating_date (datetime.date): The date of the rating.

    Validates the existence of hospitals, inserts missing records into the `location`
    and `hospital` tables, and adds a quality rating to the `hospital_quality` table.
    """
    facility_id = row['Facility ID']
    hospital_name = row['Facility Name']
    city, state, zip_code = row['City'], row['State'], row['ZIP Code']
    ownership = row['Hospital Ownership']
    emergency_services = parse_boolean(row['Emergency Services'])
    hospital_type = row['Hospital Type']
    quality_rating = parse_quality_rating(row['Hospital overall rating'])

    # Ensure the `facility_id` exists in `hospital`
    cursor.execute("SELECT 1 FROM hospital WHERE hospital_pk = %s", (facility_id,))
    if not cursor.fetchone():
        # Insert into `location`
        cursor.execute("""
            INSERT INTO location (city, state, zip_code)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (city, state, zip_code))
        location_id = cursor.fetchone()[0] if cursor.rowcount else None

        # Insert into `hospital`
        cursor.execute("""
            INSERT INTO hospital (hospital_pk, hospital_name, location_id)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (facility_id, hospital_name, location_id))


    # Insert into `hospital_quality`
    cursor.execute("""
        INSERT INTO hospital_quality (
            facility_id, quality_rating, rating_date, ownership, hospital_type, provides_emergency_services
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (facility_id, quality_rating, rating_date, ownership, hospital_type, emergency_services))


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
