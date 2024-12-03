# **Hospital Data Pipeline Project (Data Engineering)** 🚑

## **Project Overview**

This project implements a robust data pipeline to load, process, and store data related to hospitals across the United States. Weekly data from the U.S. Department of Health and Human Services (HHS) and periodic data from the Centers for Medicare and Medicaid Services (CMS) are ingested, transformed, and stored in a PostgreSQL database. This enables efficient querying and analysis of hospital capacity, quality, and COVID-19 impacts.

The project allows for:
- **Automated ingestion** of large datasets.
- **Streamlined transformation** and normalization of messy data.
- **Insightful reporting** on healthcare capacity and COVID-19 trends.

## **Dataset Description**

The project uses two main datasets:

### **HHS Hospital Data**
- Updated **weekly**, covering:
  - Hospital capacity metrics.
  - COVID-19 patient counts.
  - ICU availability and usage.
  - Bed availability.

### **CMS Quality Data**
- Updated **periodically**, providing:
  - Hospital quality ratings.
  - Ownership and facility characteristics.
  - Emergency services availability.

### **Key Data Fields**

- **Hospital Information**:
  - **Unique Hospital ID** (`hospital_pk`): A unique identifier for each hospital.
  - **State** (`state`): State abbreviation (e.g., PA).
  - **Hospital Name** (`hospital_name`): Name of the hospital.
  - **Address Details**: Includes street address, city, ZIP code, and FIPS code (county identifier).
  - **Location Coordinates**: Geocoded latitude and longitude extracted from `geocoded_hospital_address`.

- **Weekly Metrics**:
  - **Collection Week** (`collection_week`): The specific week of data collection.
  - **Hospital Bed Availability**:
    - Adult beds (`all_adult_hospital_beds_7_day_avg`)
    - Pediatric beds (`all_pediatric_inpatient_beds_7_day_avg`)
  - **Beds in Use**:
    - Adult beds in use (`all_adult_hospital_inpatient_bed_occupied_7_day_avg`)
    - Pediatric beds in use (`all_pediatric_inpatient_bed_occupied_7_day_avg`)
  - **ICU Beds**:
    - Total ICU beds available (`total_icu_beds_7_day_avg`)
    - ICU beds in use (`icu_beds_used_7_day_avg`)
  - **COVID-19 Patient Counts**:
    - Total inpatient beds used by COVID patients (`inpatient_beds_used_covid_7_day_avg`)
    - Adult ICU patients with confirmed COVID (`staffed_icu_adult_patients_confirmed_covid_7_day_avg`)

- **Hospital Quality Data**:
  - **Ownership Type**: Type of hospital ownership (e.g., private, government).
  - **Hospital Type**: Facility type (e.g., general hospital, critical access hospital).
  - **Emergency Services**: Availability of emergency services.
  - **Quality Rating**: Overall quality rating on a scale of 1–5.

## **Project Structure**

1. **Database Setup**:
   - SQL scripts are provided to create the database structure for efficient data storage.
   - **Key tables** include:
     - `hospital`
     - `location`
     - `hospital_quality`
     - `weekly_report`

2. **Data Loading Scripts**:
   - **`load-hhs.py`**:
     - Processes HHS weekly hospital data.
     - Extracts geocoded addresses into latitude and longitude.
     - Manages NULL values and inserts clean data into tables.
   - **`load-quality.py`**:
     - Processes CMS quality data.
     - Updates or inserts hospital quality and service details into the database.

## **Instructions to Run the Project**

1. **Set Up the Database**:
   - Run the provided SQL scripts to set up the PostgreSQL database.
   - Ensure the following tables are created:
     - `hospital`
     - `location`
     - `hospital_quality`
     - `weekly_report`

2. **Load Data**:
   - Place the HHS and CMS data CSV files in the designated directory.
   - Run the ingestion scripts:

     **For HHS Weekly Data**:
     ```bash
     python load-hhs.py [weekly_hhs_data.csv]
     ```

     **For CMS Quality Data**:
     ```bash
     python load-quality.py [quality_data.csv]
     ```

## **Automated Reporting System**

The project includes an automated reporting system (`weekly-report.py`) that generates a weekly PDF report summarizing hospital metrics.

### **Command to Run**:
```bash
python weekly-report.py <YYYY-MM-DD>
```
- **Report Includes**:
   -Hospital records summary for the selected week.
   -Bed utilization trends over the past 5 weeks.
   -Percent of beds used by hospital quality rating.
   -States with the fewest open beds.
   -Total bed usage over time (all cases and COVID-specific).
   -Hospitals not reporting data in the past week.

## **Project Objectives**

- **Streamline Data Processing**:
  - Normalize and clean datasets for accurate storage and retrieval.
  - Handle NULL values and geocoded data transformations.
- **Enable Analytical Insights**:
  - Maintain historical hospital quality ratings for trend analysis.
  - Enable fast, accurate querying on key metrics.
- **Automate Reporting**:
  - Provide timely, graphical reports summarizing hospital utilization, bed capacity, and COVID-19 impacts.

## **Requirements**

1. **Python**: Version 3.8+
2. **PostgreSQL**: Ensure database access and permissions.
3. **Libraries**: Install required Python libraries:
   ```bash
   pip install pandas matplotlib psycopg

> **Note**: Update your credentials in `credentials.py` and make sure the required libraries (e.g., `psycopg`, `pandas`) are installed before running scripts.


## Acknowledgements
- Data Source: U.S. Department of Health and Human Services (HHS) and Centers for Medicare and Medicaid Services (CMS).
- Centers for Medicare and Medicaid Services (CMS).
---

