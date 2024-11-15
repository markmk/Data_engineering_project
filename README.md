# Hospital Data Pipeline Project (Data Engineering)

## Project Overview
This project creates a robust data pipeline to load, process, and store data related to hospitals across the United States. Each week, data from the U.S. Department of Health and Human Services (HHS) is ingested, transformed, and stored in a PostgreSQL database, enabling efficient querying and analysis.

The data includes key metrics on hospital capacity, COVID-19 patient counts, ICU availability, and general quality ratings, allowing for timely insights into healthcare capacity and COVID-19 impacts.

## Dataset Description
The project uses two main datasets:
1. **HHS Hospital Data** - Updated weekly, covering metrics related to hospital capacity, COVID-19 patients, ICU usage, and bed availability.
2. **CMS Quality Data** - Updated periodically, with details on hospital quality ratings, ownership, emergency services, and other facility characteristics.

### Key Data Fields
1. **Hospital Information**:
   - **Unique Hospital ID** (`hospital_pk`): A unique identifier for each hospital.
   - **State** (`state`): State abbreviation (e.g., PA).
   - **Hospital Name** (`hospital_name`): Name of the hospital.
   - **Address Details**: Includes street address, city, ZIP code, and FIPS code (county identifier).
   - **Location Coordinates**: Geocoded latitude and longitude from `geocoded_hospital_address`.

2. **Weekly Metrics**:
   - **Collection Week** (`collection_week`): The specific week of data collection.
   - **Hospital Bed Availability**:
     - Total available beds, divided into adult (`all_adult_hospital_beds_7_day_avg`) and pediatric (`all_pediatric_inpatient_beds_7_day_avg`) categories.
   - **Beds in Use**:
     - Adult inpatient beds in use (`all_adult_hospital_inpatient_bed_occupied_7_day_avg`)
     - Pediatric inpatient beds in use (`all_pediatric_inpatient_bed_occupied_7_day_avg`)
   - **ICU Beds**:
     - Total ICU beds available (`total_icu_beds_7_day_avg`)
     - ICU beds currently in use (`icu_beds_used_7_day_avg`)
   - **COVID-19 Patient Counts**:
     - Total inpatient beds used by COVID patients (`inpatient_beds_used_covid_7_day_avg`)
     - Adult ICU patients with confirmed COVID (`staffed_icu_adult_patients_confirmed_covid_7_day_avg`)

3. **Hospital Quality Data**:
   - **Ownership Type**: Hospital ownership, e.g., government or private.
   - **Hospital Type**: Type of facility (e.g., general hospital).
   - **Emergency Services**: Availability of emergency services.
   - **Quality Rating**: Overall quality rating of the hospital.

## Project Structure
- **Database Setup**: Contains SQL scripts to set up and structure the database for efficient storage of HHS and CMS data.
- **Data Loading Scripts**:
  - `load-hhs.py`: Loads HHS weekly hospital data, processes geocoded addresses, manages NULL values, and inserts data into respective tables.
  - `load-quality.py`: Loads CMS quality data, processes fields related to hospital quality and services, and updates or inserts new records as needed.

## Instructions to Run the Project
1. **Set Up Database**:
   - Use the provided SQL scripts to set up the PostgreSQL database.
   - Tables include `hospital`, `location`, `hospital_quality`, and `weekly_report`.

2. **Load Data**:
   - Place the CSV files for HHS data and CMS quality data in the designated directory.
   - Run `load-hhs.py` to ingest the HHS weekly data:
     ```bash
     python load-hhs.py [weekly_hhs_data.csv]
     ```
   - Run `load-quality.py` to ingest or update CMS quality data:
     ```bash
     python load-quality.py [quality_data.csv]
     ```
## Project Objectives
- Streamline ingestion of large, messy datasets and ensure they are structured for efficient querying.
- Manage NULL values and handle geocoded data transformations for consistent storage.
- Maintain historical hospital quality ratings for longitudinal analysis.
- Enable fast, accurate reporting on key hospital metrics, supporting analysis of trends in hospital capacity and quality.

---

> **Note**: Update your credentials in `credentials.py` and make sure the required libraries (e.g., `psycopg`, `pandas`) are installed before running scripts.

## Requirements
- **Python** 3.8+
- **PostgreSQL**
- Python libraries: `pandas`, `psycopg`

## Acknowledgements
- Data Source: U.S. Department of Health and Human Services (HHS) and Centers for Medicare and Medicaid Services (CMS).

---

