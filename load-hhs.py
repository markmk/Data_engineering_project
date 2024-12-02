"""Script to load HHS data"""

import sys
from helper_load_hhs import db_connection, prep_data, insert_location, \
    get_location, insert_hospital, get_hospital, insert_weekly_report


def main():
    """Main function to run the script and insert data into all tables

    The file name of data to be inserted is the first command line argument

    Transactions are committed after completion of the queries

    If an error occurs, the program will stop
    """
    file_path = sys.argv[1]
    conn = db_connection()
    cur = conn.cursor()

    try:
        data_hhs = prep_data(file_path)
        insert_location(cur, data_hhs)
        location_ids = get_location(cur, data_hhs)
        insert_hospital(cur, data_hhs, location_ids)
        hospital_ids = get_hospital(cur, data_hhs)
        insert_weekly_report(cur, data_hhs, hospital_ids)
        conn.commit()
        print(f"Data on {data_hhs.shape[0]} unique hospitals successfully inserted for week: {sys.argv[1][0:10]}")

    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
