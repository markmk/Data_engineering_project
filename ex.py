import sys
import logging
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import psycopg
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec

import credentials

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Suppress INFO messages from matplotlib about categorical units
logging.getLogger('matplotlib').setLevel(logging.WARNING)

# Database Configuration
DB_CONFIG = {
    'host': credentials.DB_HOST,
    'dbname': credentials.DB_NAME,
    'user': credentials.DB_USER,
    'password': credentials.DB_PASSWORD
}


def execute_query(query, conn, params=None):
    """
    Execute a SQL query and return the result as a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.
        conn (psycopg.Connection): The database connection object.
        params (list, optional): Parameters to pass with the query.

    Returns:
        pd.DataFrame: DataFrame containing the query results.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=colnames)
        logging.info(
            f"Executed query: {query.strip().splitlines()[0]}... "
            f"Retrieved {len(df)} records."
        )
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return pd.DataFrame()


def plot_beds_utilization(ax, df):
    """
    Plot beds utilization by hospital quality rating on the given Axes.

    Args:
        ax (matplotlib.axes.Axes): Axes object to plot on.
        df (pd.DataFrame): DataFrame with 'quality_rating' and 'percent_beds_in_use' columns.
    """
    if df.empty:
        logging.warning("No data available for Beds Utilization by Quality Rating plot.")
        return
    # Convert data types
    df['quality_rating'] = df['quality_rating'].astype(str)
    df['percent_beds_in_use'] = pd.to_numeric(df['percent_beds_in_use'], errors='coerce')

    ax.bar(df["quality_rating"], df["percent_beds_in_use"], color="teal")
    ax.set_title("Beds Utilization by Hospital Quality Rating")
    ax.set_xlabel("Hospital Quality Rating")
    ax.set_ylabel("Percent of Beds in Use (%)")
    # Format y-axis to show one decimal place
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f'{x:.1f}')
    )


def plot_total_beds_used_over_time(ax, df):
    """
    Plot the total beds used over time on the given Axes.

    Args:
        ax (matplotlib.axes.Axes): Axes object to plot on.
        df (pd.DataFrame): DataFrame with 'collection_week', 'total_beds_used_all_cases',
            and 'total_beds_used_covid_cases' columns.
    """
    if df.empty:
        logging.warning("No data available for Total Beds Used Over Time plot.")
        return
    # Convert data types
    df['collection_week'] = pd.to_datetime(df['collection_week'])
    df['total_beds_used_all_cases'] = pd.to_numeric(
        df['total_beds_used_all_cases'], errors='coerce'
    )
    df['total_beds_used_covid_cases'] = pd.to_numeric(
        df['total_beds_used_covid_cases'], errors='coerce'
    )

    ax.plot(
        df["collection_week"], df["total_beds_used_all_cases"],
        label="All Cases", marker='o'
    )
    ax.plot(
        df["collection_week"], df["total_beds_used_covid_cases"],
        label="COVID Cases", marker='o'
    )

    # Use date formatter
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig = ax.get_figure()
    fig.autofmt_xdate()

    ax.set_title("Total Beds Used Over Time")
    ax.set_xlabel("Week")
    ax.set_ylabel("Number of Beds Used")
    ax.legend()


def create_table_pdf(df, title, pdf):
    """
    Create a table in the PDF report with dynamically adjusted column widths.

    Args:
        df (pd.DataFrame): DataFrame containing the data to display.
        title (str): Title for the table.
        pdf (PdfPages): PDF object to save the table.
    """
    if df.empty:
        logging.warning(f"No data available for {title}.")
        return

    # Format numeric columns to one decimal place
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].astype(float).round(1)
        # Add thousand separators for large numbers
        if df[col].abs().max() >= 1000:
            df[col] = df[col].apply(lambda x: f'{x:,.1f}' if pd.notnull(x) else '')
        else:
            df[col] = df[col].apply(lambda x: f'{x:.1f}' if pd.notnull(x) else '')

    # Dynamically adjust the column widths
    col_widths = [
        max(len(str(value)) for value in df[col].values) for col in df.columns
    ]
    max_col_width = max(col_widths) + 2
    total_width = sum(col_widths) + len(df.columns) * 2

    plt.figure(figsize=(max(total_width / 8, 12), max(len(df) * 0.4 + 2, 6)))
    plt.axis('off')
    table = plt.table(
        cellText=df.values.tolist(),
        colLabels=df.columns.tolist(),
        loc='center',
        cellLoc='center',
        colLoc='center'
    )

    # Adjust font size and column widths
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    for i, width in enumerate(col_widths):
        table.auto_set_column_width(i)  # Adjust individual column width
        col_width = min(width, max_col_width)
        table.auto_set_column_width(i)

    plt.title(title, fontsize=14)
    pdf.savefig()
    plt.close()
    logging.info(f"{title} table added to PDF.")


def plot_hospital_utilization_by_state(ax, df):
    """
    Plot hospital utilization by state over time for top 10 states on the given Axes.

    Args:
        ax (matplotlib.axes.Axes): Axes object to plot on.
        df (pd.DataFrame): DataFrame with 'state', 'collection_week',
            and 'percent_utilization' columns.
    """
    if df.empty:
        logging.warning("No data available for Hospital Utilization by State plot.")
        return

    # Convert data types
    df['collection_week'] = pd.to_datetime(df['collection_week'])
    df = df.dropna(subset=['percent_utilization'])
    df['percent_utilization'] = pd.to_numeric(
        df['percent_utilization'], errors='coerce'
    )

    # Get the latest date
    latest_date = df['collection_week'].max()
    # Find the top 10 states by utilization in the latest week
    latest_data = df[df['collection_week'] == latest_date]
    latest_data = latest_data.dropna(subset=['percent_utilization'])
    latest_data['percent_utilization'] = latest_data['percent_utilization'].astype(float)
    top_states = latest_data.nlargest(10, 'percent_utilization')['state'].unique()
    # Filter dataframe to only include top states
    df_filtered = df[df['state'].isin(top_states)].copy()

    # Ensure df_filtered is not empty
    if df_filtered.empty:
        logging.warning("No data available after filtering for top states.")
        return

    for state in top_states:
        state_df = df_filtered[df_filtered['state'] == state]
        state_df = state_df.sort_values('collection_week')
        if state_df.empty or state_df['percent_utilization'].isnull().all():
            continue  # Skip if no data
        last_value = state_df['percent_utilization'].iloc[-1]
        ax.plot(
            state_df['collection_week'],
            state_df['percent_utilization'],
            label=f"{state} ({last_value:.1f}%)"
        )

    # Use date formatter
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig = ax.get_figure()
    fig.autofmt_xdate()

    ax.set_title(
        "Hospital Utilization by State Over Time\n"
        "(Top 10 States by Current Utilization)", fontsize=12
    )
    ax.set_xlabel("Week", fontsize=10)
    ax.set_ylabel("Percent Utilization (%)", fontsize=10)
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f'{x:.1f}')
    )
    ax.legend(loc='upper left', fontsize='small')


def add_text_ax(ax, title, text):
    """
    Add text content to the given Axes.

    Args:
        ax (matplotlib.axes.Axes): Axes object to place the text on.
        title (str): Title of the text section.
        text (str): Body text to include.
    """
    ax.axis('off')
    ax.set_title(title, fontsize=20, loc='left')
    ax.text(0, 0.5, text, fontsize=12, ha='left', wrap=True)


# Adjusted Queries (same as provided in your original code)
QUERIES = {
    # Include your original queries here
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
        COALESCE(LAG(hospital_count) OVER (ORDER BY collection_week), 0) AS previous_week_count,
        hospital_count - COALESCE(LAG(hospital_count) OVER (ORDER BY collection_week), 0) AS week_difference
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
            , 1
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
    ORDER BY h.hospital_name ASC
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
            , 1
        ) AS percent_utilization
    FROM weekly_report wr
    JOIN hospital h ON wr.hospital_weekly_id = h.hospital_pk
    JOIN location loc ON h.location_id = loc.id
    WHERE wr.collection_week <= %s
    GROUP BY wr.collection_week, loc.state
    ORDER BY wr.collection_week, loc.state;
    """
}


def generate_report(selected_date, conn):
    """
    Generate the COVID-19 weekly report PDF.

    Args:
        selected_date (datetime.date): The week-ending date for the report.
        conn (psycopg.Connection): The database connection object.
    """
    selected_date_str = selected_date.strftime('%Y-%m-%d')

    # Fetch data
    data_frames = {}
    for key, query in QUERIES.items():
        data_frames[key] = execute_query(query, conn, [selected_date_str])

    # Output filename
    report_filename = f"report-{selected_date_str}.pdf"

    # Initialize PDF
    pdf = PdfPages(report_filename)
    page_number = 1

    # Content list to manage pages
    content_list = []

    # Cover Page and Introduction
    fig = plt.figure(figsize=(8.27, 11.69))  # A4 size in inches
    ax_title = fig.add_subplot(111)
    ax_title.axis('off')
    ax_title.text(0.1, 0.9, "HHS COVID-19 Weekly Report", fontsize=24, ha='left')
    ax_title.text(0.1, 0.85, f"Week Ending: {selected_date_str}", fontsize=18, ha='left')
    ax_title.text(0.1, 0.8, "Department of Health and Human Services", fontsize=14, ha='left')
    # Add page number
    fig.text(0.9, 0.02, f'Page {page_number}', ha='right', fontsize=8)
    pdf.savefig(fig)
    plt.close(fig)
    page_number += 1
    logging.info("Cover page added to PDF.")

    # Introduction Text
    fig = plt.figure(figsize=(8.27, 11.69))
    ax_intro = fig.add_subplot(111)
    add_text_ax(
        ax_intro,
        "Introduction",
        "This report provides a summary of the COVID-19 situation based on hospital bed "
        "utilization and availability as of the selected week. It includes analyses on "
        "hospital records, bed utilization, and additional insights to aid in decision-making."
    )
    # Add page number
    fig.text(0.9, 0.02, f'Page {page_number}', ha='right', fontsize=8)
    pdf.savefig(fig)
    plt.close(fig)
    page_number += 1

    # Prepare content sections
    content_sections = []

    # 1. Hospital Records Summary
    hospital_records_df = data_frames.get("hospital_records_summary")
    if hospital_records_df is not None and not hospital_records_df.empty:
        hospital_records_df['collection_week'] = pd.to_datetime(
            hospital_records_df['collection_week']
        ).dt.strftime('%Y-%m-%d')
        content_sections.append(('table', hospital_records_df, "Hospital Records Summary"))
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
        beds_summary_df['Week'] = pd.to_datetime(
            beds_summary_df['Week']
        ).dt.strftime('%Y-%m-%d')
        content_sections.append(('table', beds_summary_df, "Beds Summary (Last 5 Weeks)"))
    else:
        logging.warning("No data available for Beds Summary.")

    # 3. Beds Utilization by Quality Rating
    beds_utilization_df = data_frames.get("beds_utilization")
    if beds_utilization_df is not None and not beds_utilization_df.empty:
        beds_utilization_df['percent_beds_in_use'] = pd.to_numeric(
            beds_utilization_df['percent_beds_in_use'], errors='coerce'
        )
        content_sections.append(('plot', beds_utilization_df, "Beds Utilization by Quality Rating"))
    else:
        logging.warning("No data available for Beds Utilization by Quality Rating.")

    # 4. Total Beds Used Over Time
    total_beds_used_df = data_frames.get("total_beds_used_over_time")
    if total_beds_used_df is not None and not total_beds_used_df.empty:
        content_sections.append(('plot', total_beds_used_df, "Total Beds Used Over Time"))
    else:
        logging.warning("No data available for Total Beds Used Over Time.")

    # Additional Analysis 1: States with Fewest Open Beds
    fewest_open_beds_df = data_frames.get("states_fewest_open_beds")
    if fewest_open_beds_df is not None and not fewest_open_beds_df.empty:
        fewest_open_beds_df = fewest_open_beds_df.rename(columns={
            'state': 'State',
            'open_beds': 'Number of Open Beds'
        })
        content_sections.append(
            ('table', fewest_open_beds_df,
             "States with Fewest Open Beds (Negative = Beds Oversubscribed)")
        )
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
        hospitals_not_reporting_df['Last Reported Week'] = pd.to_datetime(
            hospitals_not_reporting_df['Last Reported Week']
        ).dt.strftime('%Y-%m-%d')
        content_sections.append(
            ('table', hospitals_not_reporting_df,
             "Hospitals Not Reporting Data in the Past Week")
        )
    else:
        logging.warning("No data available for Hospitals Not Reporting Data.")

    # Additional Analysis 3: Hospital Utilization by State Over Time
    hospital_utilization_df = data_frames.get("hospital_utilization_by_state_over_time")
    if hospital_utilization_df is not None and not hospital_utilization_df.empty:
        content_sections.append(
            ('plot', hospital_utilization_df, "Hospital Utilization by State Over Time")
        )
    else:
        logging.warning("No data available for Hospital Utilization by State Over Time.")

    # Divide content into pages
    max_elements_per_page = 2  # Adjust based on how much content fits on a page
    for i in range(0, len(content_sections), max_elements_per_page):
        fig = plt.figure(figsize=(8.27, 11.69))
        gs = GridSpec(max_elements_per_page, 1, figure=fig)
        plt.subplots_adjust(hspace=0.5)

        for j, (content_type, df_or_data, title) in enumerate(
            content_sections[i:i + max_elements_per_page]
        ):
            ax = fig.add_subplot(gs[j, 0])
            if content_type == 'table':
                create_table_ax(ax, df_or_data, title)
            elif content_type == 'plot':
                if title == "Beds Utilization by Quality Rating":
                    plot_beds_utilization(ax, df_or_data)
                elif title == "Total Beds Used Over Time":
                    plot_total_beds_used_over_time(ax, df_or_data)
                elif title == "Hospital Utilization by State Over Time":
                    plot_hospital_utilization_by_state(ax, df_or_data)
        # Add page number
        fig.text(0.9, 0.02, f'Page {page_number}', ha='right', fontsize=8)
        pdf.savefig(fig)
        plt.close(fig)
        page_number += 1

    # Conclusion
    fig = plt.figure(figsize=(8.27, 11.69))
    ax_conclusion = fig.add_subplot(111)
    add_text_ax(
        ax_conclusion,
        "Conclusion",
        "This concludes the weekly report. The data presented aims to inform "
        "decision-making and highlight areas requiring attention."
    )
    # Add page number
    fig.text(0.9, 0.02, f'Page {page_number}', ha='right', fontsize=8)
    pdf.savefig(fig)
    plt.close(fig)
    page_number += 1

    pdf.close()
    logging.info(f"Report saved as {report_filename}")
    logging.info(f"Report generation completed: {report_filename}")


def main():
    """
    The main entry point of the script.

    Parses command-line arguments and initiates the report generation process.
    """
    if len(sys.argv) < 2:
        print("Usage: python ex.py <date>")
        sys.exit(1)

    selected_date_str = sys.argv[1]
    try:
        selected_date = datetime.strptime(selected_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD (e.g., 2022-10-21).")
        sys.exit(1)

    # Establish database connection
    try:
        conn = psycopg.connect(**DB_CONFIG, autocommit=True)
        logging.info("Database connection established.")
        generate_report(selected_date, conn)
    except Exception as e:
        logging.error(
            "An error occurred while connecting to the database or generating the report: "
            f"{e}"
        )
        sys.exit(1)
    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()