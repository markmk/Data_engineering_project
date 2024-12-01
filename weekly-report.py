import sys
import logging
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import psycopg
import credentials

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Database Configuration
DB_CONFIG = {
    'host': credentials.DB_HOST,
    'dbname': credentials.DB_NAME,
    'user': credentials.DB_USER,
    'password': credentials.DB_PASSWORD
}

# Execute SQL Query
def execute_query(query, conn, params=None):
    """
    Executes a SQL query and returns the result as a pandas DataFrame.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=colnames)
        logging.info(f"Executed query: {query.splitlines()[0]}... Retrieved {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return pd.DataFrame()

# Visualization and Report Functions
def plot_beds_utilization(df, pdf):
    """
    Plots the beds utilization by hospital quality rating.
    """
    if df.empty:
        logging.warning("No data available for Beds Utilization by Quality Rating plot.")
        return
    plt.figure(figsize=(10, 6))
    plt.bar(df["quality_rating"].astype(str), df["percent_beds_in_use"], color="teal")
    plt.title("Beds Utilization by Hospital Quality Rating")
    plt.xlabel("Hospital Quality Rating")
    plt.ylabel("Percent of Beds in Use (%)")
    plt.tight_layout()
    pdf.savefig()
    plt.close()
    logging.info("Beds Utilization by Quality Rating plot added to PDF.")

def plot_total_beds_used_over_time(df, pdf):
    """
    Plots the total beds used over time.
    """
    if df.empty:
        logging.warning("No data available for Total Beds Used Over Time plot.")
        return
    plt.figure(figsize=(12, 6))
    plt.plot(df["collection_week"], df["total_beds_used_all_cases"], label="All Cases", marker='o')
    plt.plot(df["collection_week"], df["total_beds_used_covid_cases"], label="COVID Cases", marker='o')
    plt.xticks(rotation=45)
    plt.title("Total Beds Used Over Time")
    plt.xlabel("Week")
    plt.ylabel("Number of Beds Used")
    plt.legend()
    plt.tight_layout()
    pdf.savefig()
    plt.close()
    logging.info("Total Beds Used Over Time plot added to PDF.")

def create_table_pdf(df, title, pdf):
    """
    Creates a table in the PDF report.
    """
    if df.empty:
        logging.warning(f"No data available for {title}.")
        return
    plt.figure(figsize=(12, max(len(df) * 0.4 + 2, 6)))
    plt.axis('off')
    table = plt.table(
        cellText=df.values,
        colLabels=df.columns,
        loc='center',
        cellLoc='center',
        colLoc='center'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    plt.title(title, fontsize=14)
    pdf.savefig()
    plt.close()
    logging.info(f"{title} table added to PDF.")

def plot_hospital_utilization_by_state(df, pdf):
    """
    Plots hospital utilization by state over time.
    """
    if df.empty:
        logging.warning("No data available for Hospital Utilization by State plot.")
        return
    plt.figure(figsize=(14, 8))
    states = df['state'].unique()
    for state in states:
        state_df = df[df['state'] == state]
        plt.plot(state_df['collection_week'], state_df['percent_utilization'], label=state)
    plt.xticks(rotation=45)
    plt.title("Hospital Utilization by State Over Time")
    plt.xlabel("Week")
    plt.ylabel("Percent Utilization (%)")
    plt.legend(loc="upper left", bbox_to_anchor=(1,1), fontsize='small')
    plt.tight_layout()
    pdf.savefig()
    plt.close()
    logging.info("Hospital Utilization by State plot added to PDF.")

def add_text_page(pdf, title, text):
    """
    Adds a page with text to the PDF report.
    """
    plt.figure(figsize=(8.5, 11))
    plt.axis('off')
    plt.title(title, fontsize=20, loc='left')
    plt.text(0.1, 0.8, text, fontsize=12, ha='left', wrap=True)
    pdf.savefig()
    plt.close()
    logging.info(f"Text page '{title}' added to PDF.")

# Adjusted Queries
QUERIES = {
    "hospital_records_summary": """
    WITH weekly_counts AS (
        SELECT
            collection_week,
            COUNT(DISTINCT hospital_weekly_id) AS hospital_count
        FROM weekly_report
        GROUP BY collection_week
    )
    SELECT
        collection_week,
        hospital_count,
        LAG(hospital_count) OVER (ORDER BY collection_week) AS previous_week_count,
        (hospital_count - LAG(hospital_count) OVER (ORDER BY collection_week)) AS week_difference
    FROM weekly_counts
    WHERE collection_week = (
        SELECT MAX(collection_week) FROM weekly_report WHERE collection_week <= %s
    );
    """,
    "beds_summary": """
    WITH recent_weeks AS (
        SELECT DISTINCT collection_week
        FROM weekly_report
        WHERE collection_week <= %s
        ORDER BY collection_week DESC
        LIMIT 5
    )
    SELECT
        wr.collection_week,
        SUM(wr.all_adult_hospital_beds_7_day_avg) AS total_adult_beds_available,
        SUM(wr.all_pediatric_inpatient_beds_7_day_avg) AS total_pediatric_beds_available,
        SUM(wr.all_adult_hospital_inpatient_bed_occupied_7_day_avg) AS total_adult_beds_occupied,
        SUM(wr.all_pediatric_inpatient_bed_occupied_7_day_avg) AS total_pediatric_beds_occupied,
        SUM(wr.inpatient_beds_used_covid_7_day_avg) AS total_covid_beds_used
    FROM weekly_report wr
    JOIN recent_weeks rw ON wr.collection_week = rw.collection_week
    GROUP BY wr.collection_week
    ORDER BY wr.collection_week DESC;
    """,
    "beds_utilization": """
    SELECT
        hq.quality_rating,
        ROUND(
            CAST(
                (SUM(wr.all_adult_hospital_inpatient_bed_occupied_7_day_avg + 
                     wr.all_pediatric_inpatient_bed_occupied_7_day_avg) * 100.0 /
                NULLIF(SUM(wr.all_adult_hospital_beds_7_day_avg + wr.all_pediatric_inpatient_beds_7_day_avg), 0))
                AS NUMERIC)
            , 2
        ) AS percent_beds_in_use
    FROM (
        SELECT DISTINCT ON (facility_id)
            facility_id,
            quality_rating
        FROM hospital_quality
        ORDER BY facility_id, rating_date DESC
    ) hq
    JOIN weekly_report wr ON hq.facility_id = wr.hospital_weekly_id
    WHERE wr.collection_week = (
        SELECT MAX(collection_week) FROM weekly_report WHERE collection_week <= %s
    )
    GROUP BY hq.quality_rating
    ORDER BY hq.quality_rating;
    """,
    "total_beds_used_over_time": """
    SELECT
        collection_week,
        SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg + all_pediatric_inpatient_bed_occupied_7_day_avg) AS total_beds_used_all_cases,
        SUM(inpatient_beds_used_covid_7_day_avg) AS total_beds_used_covid_cases
    FROM weekly_report
    WHERE collection_week <= %s
    GROUP BY collection_week
    ORDER BY collection_week;
    """,
    "states_fewest_open_beds": """
    SELECT
        loc.state,
        SUM(wr.all_adult_hospital_beds_7_day_avg + wr.all_pediatric_inpatient_beds_7_day_avg) -
        SUM(wr.all_adult_hospital_inpatient_bed_occupied_7_day_avg + wr.all_pediatric_inpatient_bed_occupied_7_day_avg) AS open_beds
    FROM weekly_report wr
    JOIN hospital h ON wr.hospital_weekly_id = h.hospital_pk
    JOIN location loc ON h.location_id = loc.id
    WHERE wr.collection_week = (
        SELECT MAX(collection_week) FROM weekly_report WHERE collection_week <= %s
    )
    GROUP BY loc.state
    ORDER BY open_beds ASC
    LIMIT 10;
    """,
    "hospitals_not_reporting": """
    SELECT
        h.hospital_name,
        loc.city,
        loc.state,
        MAX(wr.collection_week) AS last_reported_week
    FROM hospital h
    JOIN location loc ON h.location_id = loc.id
    LEFT JOIN weekly_report wr ON h.hospital_pk = wr.hospital_weekly_id
    GROUP BY h.hospital_name, loc.city, loc.state
    HAVING MAX(wr.collection_week) < (
        SELECT MAX(collection_week) FROM weekly_report WHERE collection_week <= %s
    )
    ORDER BY last_reported_week DESC NULLS FIRST
    LIMIT 10;
    """,
    "hospital_utilization_by_state_over_time": """
    SELECT
        wr.collection_week,
        loc.state,
        ROUND(
            CAST(
                SUM(wr.all_adult_hospital_inpatient_bed_occupied_7_day_avg + wr.all_pediatric_inpatient_bed_occupied_7_day_avg) * 100.0 /
                NULLIF(SUM(wr.all_adult_hospital_beds_7_day_avg + wr.all_pediatric_inpatient_beds_7_day_avg), 0)
                AS NUMERIC)
            , 2
        ) AS percent_utilization
    FROM weekly_report wr
    JOIN hospital h ON wr.hospital_weekly_id = h.hospital_pk
    JOIN location loc ON h.location_id = loc.id
    WHERE wr.collection_week <= %s
    GROUP BY wr.collection_week, loc.state
    ORDER BY wr.collection_week, loc.state;
    """
}

# Generate Report
def generate_report(selected_date, conn):
    """
    Generates the COVID-19 weekly report PDF.
    """
    selected_date_str = selected_date.strftime('%Y-%m-%d')

    # Fetch data
    data_frames = {}
    for key, query in QUERIES.items():
        data_frames[key] = execute_query(query, conn, [selected_date_str])

    # Output filename
    report_filename = f"report-{selected_date_str}.pdf"

    # Generate Report
    with PdfPages(report_filename) as pdf:
        # Cover Page
        plt.figure(figsize=(8.5, 11))
        plt.axis('off')
        plt.text(0.1, 0.9, "HHS COVID-19 Weekly Report", fontsize=24, ha='left')
        plt.text(0.1, 0.85, f"Week Ending: {selected_date_str}", fontsize=18, ha='left')
        plt.text(0.1, 0.8, "Department of Health and Human Services", fontsize=14, ha='left')
        pdf.savefig()
        plt.close()
        logging.info("Cover page added to PDF.")

        # Explanatory Text
        add_text_page(pdf, "Introduction", "This report provides a summary of the COVID-19 situation based on hospital bed utilization and availability as of the selected week. It includes analyses on hospital records, bed utilization, and additional insights to aid in decision-making.")

        # 1. Hospital Records Summary
        hospital_records_df = data_frames.get("hospital_records_summary")
        if hospital_records_df is not None and not hospital_records_df.empty:
            create_table_pdf(hospital_records_df, "Hospital Records Summary", pdf)
        else:
            logging.warning("No data available for Hospital Records Summary.")

        # 2. Beds Summary Table
        beds_summary_df = data_frames.get("beds_summary")
        if beds_summary_df is not None and not beds_summary_df.empty:
            beds_summary_df = beds_summary_df.rename(columns={
                'collection_week': 'Week',
                'total_adult_beds_available': 'Adult Beds Available',
                'total_pediatric_beds_available': 'Pediatric Beds Available',
                'total_adult_beds_occupied': 'Adult Beds Occupied',
                'total_pediatric_beds_occupied': 'Pediatric Beds Occupied',
                'total_covid_beds_used': 'COVID Beds Used'
            })
            create_table_pdf(beds_summary_df, "Beds Summary (Last 5 Weeks)", pdf)
        else:
            logging.warning("No data available for Beds Summary.")

        # 3. Beds Utilization by Quality Rating
        beds_utilization_df = data_frames.get("beds_utilization")
        if beds_utilization_df is not None and not beds_utilization_df.empty:
            plot_beds_utilization(beds_utilization_df, pdf)
        else:
            logging.warning("No data available for Beds Utilization by Quality Rating.")

        # 4. Total Beds Used Over Time
        total_beds_used_df = data_frames.get("total_beds_used_over_time")
        if total_beds_used_df is not None and not total_beds_used_df.empty:
            plot_total_beds_used_over_time(total_beds_used_df, pdf)
        else:
            logging.warning("No data available for Total Beds Used Over Time.")

        # Additional Analysis 1: States with Fewest Open Beds
        fewest_open_beds_df = data_frames.get("states_fewest_open_beds")
        if fewest_open_beds_df is not None and not fewest_open_beds_df.empty:
            fewest_open_beds_df = fewest_open_beds_df.rename(columns={
                'state': 'State',
                'open_beds': 'Number of Open Beds'
            })
            create_table_pdf(fewest_open_beds_df, "States with Fewest Open Beds", pdf)
        else:
            logging.warning("No data available for States with Fewest Open Beds.")

        # Additional Analysis 2: Hospitals Not Reporting Data
        hospitals_not_reporting_df = data_frames.get("hospitals_not_reporting")
        if hospitals_not_reporting_df is not None and not hospitals_not_reporting_df.empty:
            hospitals_not_reporting_df = hospitals_not_reporting_df.rename(columns={
                'hospital_name': 'Hospital Name',
                'city': 'City',
                'state': 'State',
                'last_reported_week': 'Last Reported Week'
            })
            create_table_pdf(hospitals_not_reporting_df, "Hospitals Not Reporting Data in the Past Week", pdf)
        else:
            logging.warning("No data available for Hospitals Not Reporting Data.")

        # Additional Analysis 3: Hospital Utilization by State Over Time
        hospital_utilization_df = data_frames.get("hospital_utilization_by_state_over_time")
        if hospital_utilization_df is not None and not hospital_utilization_df.empty:
            # Limit to top 5 states with highest utilization in the latest week
            latest_week = hospital_utilization_df['collection_week'].max()
            latest_data = hospital_utilization_df[hospital_utilization_df['collection_week'] == latest_week]
            top_states = latest_data.nlargest(5, 'percent_utilization')['state']
            filtered_df = hospital_utilization_df[hospital_utilization_df['state'].isin(top_states)]
            plot_hospital_utilization_by_state(filtered_df, pdf)
        else:
            logging.warning("No data available for Hospital Utilization by State Over Time.")

        # Final Page
        add_text_page(pdf, "Conclusion", "This concludes the weekly report. The data presented aims to inform decision-making and highlight areas requiring attention. For further information, please contact the Department of Health and Human Services.")

    print(f"Report saved as {report_filename}")
    logging.info(f"Report generation completed: {report_filename}")

# Entry Point
def main():
    """
    The main entry point of the script.
    """
    if len(sys.argv) < 2:
        print("Usage: python weekly-report.py <date>")
        sys.exit(1)

    selected_date_str = sys.argv[1]
    try:
        selected_date = datetime.strptime(selected_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD (e.g., 2023-09-30).")
        sys.exit(1)

    # Establish database connection
    try:
        conn = psycopg.connect(**DB_CONFIG, autocommit=True)
        logging.info("Database connection established.")
        generate_report(selected_date, conn)
    except Exception as e:
        logging.error(f"An error occurred while connecting to the database or generating the report: {e}")
        sys.exit(1)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
