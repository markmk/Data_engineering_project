"""Helper functions for load-hhs.py"""

# Import modules
import psycopg
import pandas as pd
import numpy as np
import credentials


def db_connection():
    """Connects to PostgreSQL database using credentials

    The credentials.py file should have string objects:
    DB_HOST, DB_NAME, DB_USER, and DB_PASSWORD

    Returns
    -------
    connection object
        Connection to the appropropriate SQL database
    """
    try:
        return psycopg.connect(
            host=credentials.DB_HOST, dbname=credentials.DB_NAME,
            user=credentials.DB_USER, password=credentials.DB_PASSWORD
        )
    except psycopg.errors.OperationalError as e:
        print(f"Error occurred while connecting to the database: {e}")
        raise


def prep_data(file_path):
    """Loads data file and prepares it for insertion

    Parameters
    ----------
    file_path: string
        Path of the file name to be loaded

    Returns
    -------
    data_hhs: pandas dataframe
        The cleaned dataset ready for insertion into the database
    """
    # Load data
    data_hhs = pd.read_csv(file_path)
    data_hhs = data_hhs[["hospital_pk", "state", "hospital_name", "address", "city", "zip", "fips_code",
                         "geocoded_hospital_address", "collection_week", "all_adult_hospital_beds_7_day_avg",
                         "all_pediatric_inpatient_beds_7_day_avg",
                         "all_adult_hospital_inpatient_bed_occupied_7_day_avg",
                         "all_pediatric_inpatient_bed_occupied_7_day_avg", "total_icu_beds_7_day_avg",
                         "icu_beds_used_7_day_avg", "inpatient_beds_used_covid_7_day_avg",
                         "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]]

    # Convert Pandas NAs and Nulls to None type
    data_hhs = data_hhs.where(pd.notna(data_hhs), None)
    data_hhs = data_hhs.where(pd.notnull(data_hhs), None)
    # Convert -999s to NaN (will convert to None later)
    data_hhs = data_hhs.replace(-999999, np.nan)
    # Convert lat + long columns
    data_hhs['geocoded_hospital_address'] = data_hhs['geocoded_hospital_address'].str.slice(start=7, stop=-1)
    data_hhs[['latitude', 'longitude']] = data_hhs['geocoded_hospital_address'].str.split(' ', expand=True)
    data_hhs['latitude'] = data_hhs['latitude'].astype(float)
    data_hhs['longitude'] = data_hhs['longitude'].astype(float)
    # Remove duplicate entries of hospitals based on 'hospital_pk' column
    data_hhs = data_hhs.drop_duplicates(subset='hospital_pk')
    # Convert collection week to date object
    data_hhs['collection_week'] = pd.to_datetime(data_hhs['collection_week'], format='%Y-%m-%d')

    return data_hhs


def insert_location(cur, data_hhs):
    """Query to insert data into the locaton table

    Parameters
    ----------
    cur: cursor object
        Facilitates an interaction with the connected database
    data_hhs: pandas dataframe
        All columns of the dataset containing data to be inserted
    """
    location_data = data_hhs[["city", "state", "zip", "address", "latitude", "longitude",
                              "fips_code"]].itertuples(index=False, name=None)
    # Convert NaNs to None
    location_data = (
        (city, state, zip, address, latitude if not np.isnan(latitude) else None,
         longitude if not np.isnan(longitude) else None, fips_code if not np.isnan(fips_code) else None)
        for city, state, zip, address, latitude, longitude, fips_code in location_data
    )

    try:
        cur.executemany(
            """
            INSERT INTO location (city, state, zip_code, address, latitude, longitude, fips_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, state, zip_code, address, latitude, longitude) DO NOTHING
            """,
            location_data
        )
    except psycopg.IntegrityError as e:
        print(f"Integrity error occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.DataError as e:
        print(f"Data error occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.errors.UniqueViolation as e:
        print(f"Unique constraint violation occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.Error as e:  # Generic error
        print(f"Error inserting location for row {cur.rowcount}: {e}")
        raise


def get_location(cur, data_hhs):
    """Obtain the location_ids that were inserted into the location table

    cur: cursor object
        Facilitates an interaction with the connected database
    data_hhs: pandas dataframe
        All columns of the dataset containing data to be inserted

    Returns
    -------
    list
        List of all values of location (id) that were inserted in the insert_location() function call
    """
    cities = list(data_hhs['city'])
    states = list(data_hhs['state'])
    zip_codes = list(data_hhs['zip'])
    addresses = list(data_hhs['address'])
    latitudes = list(data_hhs['latitude'])
    longitudes = list(data_hhs['longitude'])
    fips_codes = list(data_hhs['fips_code'])
    cur.execute("SELECT id FROM location "
                "WHERE (location.city, location.state, location.zip_code, location.address, "
                "location.latitude, location.longitude, location.fips_code) "
                "IN (SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::float[], %s::float[], %s::text[]))",
                (cities, states, zip_codes, addresses, latitudes, longitudes, fips_codes))
    return [row[0] for row in cur.fetchall()]


def insert_hospital(cur, data_hhs, location_ids):
    """Query to insert data into the hospital table

    Parameters
    ----------
    cur: cursor object
        Facilitates an interaction with the connected database
    data_hhs: pandas dataframe
        All columns of the dataset containing data to be inserted
    location_ids: list
        Foreign key corresponding to location (id)
    """
    hospital_data = [(hospital_pk, hospital_name, location_id)
                     for hospital_pk, hospital_name, location_id
                     in zip(data_hhs['hospital_pk'], data_hhs['hospital_name'], location_ids)]

    try:
        cur.executemany(
            """
            INSERT INTO hospital (hospital_pk, hospital_name, location_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (hospital_pk) DO NOTHING
            """,
            hospital_data
        )
    except psycopg.errors.ForeignKeyViolation as e:
        print(f"ForeignKeyViolation for row {cur.rowcount}: {e}")
        raise
    except psycopg.IntegrityError as e:
        print(f"Integrity error occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.DataError as e:
        print(f"Data error occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.errors.UniqueViolation as e:
        print(f"Unique constraint violation occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.Error as e:  # Generic error
        print(f"Error inserting hospital for row {cur.rowcount}: {e}")
        raise


def get_hospital(cur, data_hhs):
    """Obtain the location_ids that were inserted into the location table

    cur: cursor object
        Facilitates an interaction with the connected database
    data_hhs: pandas dataframe
        All columns of the dataset containing data to be inserted

    Returns
    -------
    list
        List of all values of hospital_pk that were inserted in the insert_hospital() function call
    """
    pks = list(data_hhs['hospital_pk'])
    names = list(data_hhs['hospital_name'])
    cur.execute("SELECT hospital_pk FROM hospital "
                "WHERE (hospital.hospital_pk, hospital.hospital_name) "
                "IN (SELECT * FROM unnest(%s::text[], %s::text[]))",
                (pks, names))
    return [row[0] for row in cur.fetchall()]


def insert_weekly_report(cur, data_hhs, hospital_ids):
    """Query to insert data into the weekly_report table

    Parameters
    ----------
    cur: cursor object
        Facilitates an interaction with the connected database
    data_hhs: pandas dataframe
        All columns of the dataset containing data to be inserted
    hospital_ids: list
        Foreign key corresponding to hospital (hospital_pk)
    """
    weekly_data = [(collection_week, all_adult, all_pediatric, all_icu, adult_occupied,
                    pediatric_occupied, icu_occupied, covid_total, covid_adult_icu, hospital_id)
                   for collection_week, all_adult, all_pediatric, all_icu, adult_occupied,
                   pediatric_occupied, icu_occupied, covid_total, covid_adult_icu, hospital_id
                   in zip(data_hhs['collection_week'], data_hhs['all_adult_hospital_beds_7_day_avg'],
                          data_hhs['all_pediatric_inpatient_beds_7_day_avg'],
                          data_hhs['total_icu_beds_7_day_avg'],
                          data_hhs['all_adult_hospital_inpatient_bed_occupied_7_day_avg'],
                          data_hhs['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                          data_hhs['icu_beds_used_7_day_avg'], data_hhs['inpatient_beds_used_covid_7_day_avg'],
                          data_hhs['staffed_icu_adult_patients_confirmed_covid_7_day_avg'], hospital_ids)]
    # Convert NaNs to None
    weekly_data = (
            (collection_week, all_adult if not np.isnan(all_adult) else None,
             all_pediatric if not np.isnan(all_pediatric) else None,
             all_icu if not np.isnan(all_icu) else None,
             adult_occupied if not np.isnan(adult_occupied) else None,
             pediatric_occupied if not np.isnan(pediatric_occupied) else None,
             icu_occupied if not np.isnan(icu_occupied) else None,
             covid_total if not np.isnan(covid_total) else None,
             covid_adult_icu if not np.isnan(covid_adult_icu) else None,
             hospital_id)
            for collection_week, all_adult, all_pediatric, all_icu, adult_occupied,
            pediatric_occupied, icu_occupied, covid_total, covid_adult_icu, hospital_id in weekly_data
    )

    try:
        cur.executemany(
            """
            INSERT INTO weekly_report (collection_week, all_adult_hospital_beds_7_day_avg,
            all_pediatric_inpatient_beds_7_day_avg, total_icu_beds_7_day_avg,
            all_adult_hospital_inpatient_bed_occupied_7_day_avg,
            all_pediatric_inpatient_bed_occupied_7_day_avg, icu_beds_used_7_day_avg,
            inpatient_beds_used_covid_7_day_avg, staffed_icu_adult_patients_confirmed_covid_7_day_avg,
            hospital_weekly_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            weekly_data
        )
    except psycopg.errors.ForeignKeyViolation as e:
        print(f"ForeignKeyViolation for row {cur.rowcount}: {e}")
        raise
    except psycopg.IntegrityError as e:
        print(f"Integrity error occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.DataError as e:
        print(f"Data error occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.errors.UniqueViolation as e:
        print(f"Unique constraint violation occurred for row {cur.rowcount}: {e}")
        raise
    except psycopg.Error as e:  # Generic error
        print(f"Error inserting weekly_report for row {cur.rowcount}: {e}")
        raise
