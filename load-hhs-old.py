# load-hhs.py
import psycopg
import pandas as pd
import numpy as np
import sys
import credentials


# Connect to PostgreSQL database using credentials
conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com", dbname=credentials.DB_NAME,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
    )
cur = conn.cursor()

data_hhs = pd.read_csv(sys.argv[1])
data_hhs = data_hhs[["hospital_pk", "state", "hospital_name", "address",
                     "city", "zip", "fips_code", "geocoded_hospital_address",
                     "collection_week", "all_adult_hospital_beds_7_day_avg",
                     "all_pediatric_inpatient_beds_7_day_avg",
                     "all_adult_hospital_inpatient_bed_occupied_7_day_avg",
                     "all_pediatric_inpatient_bed_occupied_7_day_avg",
                     "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg",
                     "inpatient_beds_used_covid_7_day_avg",
                     "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]]

# Convert NAs and NaNs to None
data_hhs = data_hhs.where(pd.notna(data_hhs), None)
data_hhs = data_hhs.where(pd.notnull(data_hhs), None)

# Convert -999 to NaN (convert to None later)
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

# Inserting data (Location Table)
location_data = \
    data_hhs[["city", "state", "zip", "latitude", "longitude", "fips_code"]].itertuples(index=False, name=None)
location_data = (
    (city, state, zip, latitude if not np.isnan(latitude) else None,
     longitude if not np.isnan(longitude) else None, fips_code if not np.isnan(fips_code) else None)
    for city, state, zip, latitude, longitude, fips_code in location_data
)
cur.executemany(
    "INSERT INTO location (city, state, zip_code, latitude, longitude, fips_code) "
    "VALUES (%s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (city, state, zip_code, latitude, longitude) DO NOTHING "
    "RETURNING id",
    location_data)

# Obtain location id key for each hospital's location
cities = list(data_hhs['city'])
states = list(data_hhs['state'])
zip_codes = list(data_hhs['zip'])
latitudes = list(data_hhs['latitude'])
longitudes = list(data_hhs['longitude'])
fips_codes = list(data_hhs['fips_code'])
cur.execute("SELECT id FROM location "
            "WHERE (location.city, location.state, location.zip_code, "
            "location.latitude, location.longitude, location.fips_code) "
            "IN (SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::float[], %s::float[], %s::text[]))",
            (cities, states, zip_codes, latitudes, longitudes, fips_codes))
location_ids = [row[0] for row in cur.fetchall()]


# Insert data (Hospital Table)
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
except psycopg.errors.ForeignKeyViolation:
    print("Foreign key has no corresponding primary key")
    conn.rollback()


# Obtain hospital_pk key for each hospital
pks = list(data_hhs['hospital_pk'])
names = list(data_hhs['hospital_name'])
cur.execute("SELECT hospital_pk FROM hospital "
            "WHERE (hospital.hospital_pk, hospital.hospital_name) "
            "IN (SELECT * FROM unnest(%s::text[], %s::text[]))", 
            (pks, names))
hospital_ids = [row[0] for row in cur.fetchall()]

# Insert data (Weekly_report Table)
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
        "INSERT INTO weekly_report (collection_week, all_adult_hospital_beds_7_day_avg, "
        "all_pediatric_inpatient_beds_7_day_avg, all_adult_hospital_inpatient_bed_occupied_7_day_avg, "
        "all_pediatric_inpatient_bed_occupied_7_day_avg, total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, "
        "inpatient_beds_used_covid_7_day_avg, staffed_icu_adult_patients_confirmed_covid_7_day_avg, "
        "hospital_weekly_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        weekly_data)
except psycopg.errors.ForeignKeyViolation:
    print("Foreign key has no corresponding primary key")
    conn.rollback()


conn.commit()
conn.close()
