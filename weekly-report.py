import streamlit as st
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import psycopg
import credentials
import plotly.express as px
import json
import requests
from datetime import timedelta
import matplotlib.dates as mdates
import numpy as np


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Streamlit Configuration
st.set_page_config(page_title="HHS COVID-19 Weekly Report", layout="wide")

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
        return pd.DataFrame(rows, columns=colnames)
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return pd.DataFrame()


def plot_beds_utilization_streamlit(df):
    """
    Plot beds utilization by hospital quality rating in Streamlit.

    Args:
        df (pd.DataFrame): DataFrame with 'quality_rating' and 'percent_beds_in_use' columns.
    """    
    if df.empty:
        st.warning("No data available for Beds Utilization by Quality Rating.")
        return

    # Ensure the data types are correct
    df['percent_beds_in_use'] = pd.to_numeric(df['percent_beds_in_use'], errors='coerce')

    # Create a boolean mask to filter out rows where 'percent_beds_in_use' is NaN
    valid_data = df[~np.isnan(df['quality_rating'])]

    if valid_data.empty:
        st.warning("No data available for Beds Utilization by Quality Rating.")
        return

    # Create the bar plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(valid_data["quality_rating"], valid_data["percent_beds_in_use"], color="teal")
    ax.set_title("Beds Utilization by Hospital Quality Rating")
    ax.set_xlabel("Hospital Quality Rating")
    ax.set_ylabel("Percent of Beds in Use (%)")

    # Render the plot in Streamlit
    st.pyplot(fig)


def plot_total_beds_used(df):
    """
    Plot total hospital beds used per week, split into all cases and COVID cases.

    Args:
        df (pd.DataFrame): DataFrame with 'collection_week', 'total_beds_used', and 'covid_beds_used'.
    """
    if df.empty:
        st.warning("No data available for Total Beds Used.")
        return

    # Ensure proper data types
    df['collection_week'] = pd.to_datetime(df['collection_week'])
    df.sort_values('collection_week', inplace=True)

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df['collection_week'], df['total_beds_used'], label='All Cases', marker='o')
    ax.plot(df['collection_week'], df['covid_beds_used'], label='COVID Cases', marker='o')

    # Format the x-ticks to match the selected dates
    unique_weeks = sorted(df['collection_week'].unique())
    ax.set_xticks(unique_weeks)  # Use the unique collection weeks as x-ticks
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format dates as YYYY-MM-DD
    # Formatting
    ax.set_title("Total Hospital Beds Used Per Week")
    ax.set_xlabel("Week")
    ax.set_ylabel("Number of Beds")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    ax.legend()

    # Render in Streamlit
    st.pyplot(fig)


def plot_covid_cases_map(df):
    """
    Create a map showing the number of COVID cases by state.

    Args:
        df (pd.DataFrame): DataFrame with 'state' and 'total_covid_cases' columns.

    Returns:
        None: Displays the map in Streamlit.
    """
    if df.empty:
        st.warning("No data available for COVID cases by state.")
        return

    # Ensure state is a string
    df['state'] = df['state'].astype(str)

    # Fetch GeoJSON for US states
    geojson_url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
    try:
        response = requests.get(geojson_url)
        response.raise_for_status()
        geojson = response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching GeoJSON data: {e}")
        return

    # Create the choropleth map
    fig = px.choropleth(
        df,
        geojson=geojson,
        locations='state',
        locationmode='USA-states',  # Use state abbreviations
        color='total_covid_cases',
        color_continuous_scale="Viridis",
        scope="usa",
        title="Number of COVID Cases by State"
    )

    fig.update_geos(
        fitbounds="locations",
        visible=False
    )

    # Render the map in Streamlit
    st.plotly_chart(fig, use_container_width=True)


def create_table_streamlit(df, title):
    """
    Display a table in Streamlit with dynamically adjusted column formatting.

    Args:
        df (pd.DataFrame): DataFrame containing the data to display.
        title (str): Title for the table.
    """
    if df.empty:
        st.warning(f"No data available for {title}.")
        return

    # Format numeric columns to one decimal place
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].astype(float).round(1)
        # Add thousand separators for large numbers
        if df[col].abs().max() >= 1000:
            df[col] = df[col].apply(lambda x: f'{x:,.1f}' if pd.notnull(x) else '')
        else:
            df[col] = df[col].apply(lambda x: f'{x:.1f}' if pd.notnull(x) else '')

    st.header(title)
    st.table(df)


def plot_hospital_utilization_streamlit(df):
    """
    Plot hospital utilization by state over time for top 10 states in Streamlit.

    Args:
        df (pd.DataFrame): DataFrame with 'state', 'collection_week',
            and 'percent_utilization' columns.
    """
    if df.empty:
        st.warning("No data available for Hospital Utilization by State plot.")
        return

    # Convert data types
    df['collection_week'] = pd.to_datetime(df['collection_week'])
    df = df.dropna(subset=['percent_utilization'])
    df['percent_utilization'] = pd.to_numeric(df['percent_utilization'], errors='coerce')

    # Get the latest date
    latest_date = df['collection_week'].max()
    # Find the top 10 states by utilization in the latest week
    latest_data = df[df['collection_week'] == latest_date]
    latest_data = latest_data.dropna(subset=['percent_utilization'])
    latest_data['percent_utilization'] = latest_data['percent_utilization'].astype(float)
    top_states = latest_data.nlargest(10, 'percent_utilization')['state'].unique()
    # Filter dataframe to only include top states
    df_filtered = df[df['state'].isin(top_states)]

    # Set up the figure
    unique_weeks = sorted(df['collection_week'].unique())
    # Set the x-ticks to match the available collection weeks
    fig, ax = plt.subplots(figsize=(14, 6))
    # Plot each state's data
    for state in top_states:
        state_df = df_filtered[df_filtered['state'] == state]
        plt.plot(
            state_df['collection_week'],
            state_df['percent_utilization'],
            label=f"{state} ({state_df['percent_utilization'].iloc[-1]:.1f}%)"
        )

    # Format the x-ticks to match the selected dates
    ax.set_xticks(unique_weeks)  # Use the unique collection weeks as x-ticks
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format dates as YYYY-MM-DD

    # Adjust x-axis ticks and labels
    plt.xticks(rotation=45)
    plt.title(
        "Hospital Utilization by State Over Time\n"
        "(Top 10 States by Current Utilization)", fontsize=12
    )
    plt.xlabel("Week", fontsize=10)
    plt.ylabel("Percent Utilization (%)", fontsize=10)
    plt.gca().yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f'{x:.1f}')
    )
    plt.legend(loc='center left', bbox_to_anchor=(1.05, 0.5), fontsize='small')
    plt.tight_layout(rect=[0, 0, 0.85, 1])

    st.pyplot(plt)


def add_text_streamlit(title, text):
    """
    Add a text section to the Streamlit report.

    Args:
        title (str): Title of the text section.
        text (str): Body text to include.
    """
    st.subheader(title)
    st.markdown(text)


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
        COALESCE(LAG(hospital_count) OVER (ORDER BY collection_week), 0) AS previous_week_count,
        hospital_count - COALESCE(LAG(hospital_count) OVER (ORDER BY collection_week), 0) AS week_difference
    FROM weekly_counts
    WHERE collection_week IN (%s, %s)
    ORDER BY collection_week DESC
    LIMIT 1;
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
    "weekly_beds_used": """
    SELECT
        collection_week,
        SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg +
            all_pediatric_inpatient_bed_occupied_7_day_avg) AS total_beds_used,
        SUM(inpatient_beds_used_covid_7_day_avg) AS covid_beds_used
    FROM weekly_report
    WHERE collection_week <= %s
    GROUP BY collection_week
    ORDER BY collection_week;
    """, 
    "covid_cases_by_state": """
    SELECT
        loc.state,
        SUM(wr.inpatient_beds_used_covid_7_day_avg) AS total_covid_cases
    FROM weekly_report wr
    JOIN hospital h ON wr.hospital_weekly_id = h.hospital_pk
    JOIN location loc ON h.location_id = loc.id
    GROUP BY loc.state
    ORDER BY loc.state;
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
    Generate the COVID-19 weekly report interactively in Streamlit.

    Args:
        selected_date (datetime.date): The week-ending date for the report.
        conn (psycopg.Connection): The database connection object.
    """
    previous_week = selected_date - timedelta(weeks=1)
    
    selected_date_str = selected_date.strftime('%Y-%m-%d')
    previous_week_str = previous_week.strftime('%Y-%m-%d')

    # Header
    st.header(f"HHS COVID-19 Weekly Report")
    st.subheader(f"Week Ending: {selected_date_str}")
    st.write("This report provides insights into hospital bed utilization and COVID-19 trends.")

    # 1. Hospital Records Summary
    st.markdown("### Hospital Records Summary")
    hospital_records_df = execute_query(QUERIES["hospital_records_summary"], conn, [selected_date_str, previous_week_str])
    if not hospital_records_df.empty:
        hospital_records_df['collection_week'] = pd.to_datetime(hospital_records_df['collection_week']).dt.strftime('%Y-%m-%d')
        st.table(hospital_records_df)
    else:
        st.warning("No data available for Hospital Records Summary.")

    # 2. Beds Summary
    st.markdown("### Beds Summary (Last 5 Weeks)")
    beds_summary_df = execute_query(QUERIES["beds_summary"], conn, [selected_date_str])
    if not beds_summary_df.empty:
        st.table(beds_summary_df)
    else:
        st.warning("No data available for Beds Summary.")

    # 3. Beds Utilization by Quality Rating
    st.markdown("### Beds Utilization by Quality Rating")
    beds_utilization_df = execute_query(QUERIES["beds_utilization"], conn, [selected_date_str])
    if not beds_utilization_df.empty:
        plot_beds_utilization_streamlit(beds_utilization_df)
    else:
        st.warning("No data available for Beds Utilization by Quality Rating.")

    # 4. COVID Cases by State Map
    st.markdown("### COVID Cases by State")
    covid_cases_df = execute_query(QUERIES["covid_cases_by_state"], conn)
    if not covid_cases_df.empty:
        plot_covid_cases_map(covid_cases_df)
    else:
        st.warning("No data available for COVID cases by state.")

    # 5. Total Beds Used Over Time
    st.markdown("### Total Hospital Beds Used Per Week (All Cases vs COVID Cases)")
    weekly_beds_df = execute_query(QUERIES["weekly_beds_used"], conn, [selected_date_str])
    if not weekly_beds_df.empty:
        plot_total_beds_used(weekly_beds_df)
    else:
        st.warning("No data available for Total Beds Used.")

    # Additional Analysis: States with Fewest Open Beds
    st.markdown("### States with Fewest Open Beds")
    fewest_open_beds_df = execute_query(QUERIES["states_fewest_open_beds"], conn, [selected_date_str])
    if not fewest_open_beds_df.empty:
        st.table(fewest_open_beds_df)
    else:
        st.warning("No data available for States with Fewest Open Beds.")

    # Additional Analysis: Hospitals Not Reporting Data
    st.markdown("### Hospitals Not Reporting Data")
    hospitals_not_reporting_df = execute_query(QUERIES["hospitals_not_reporting"], conn, [selected_date_str])
    if not hospitals_not_reporting_df.empty:
        st.table(hospitals_not_reporting_df)
    else:
        st.warning("No data available for Hospitals Not Reporting Data.")

    # Additional Analysis: Hospital Utilization by State Over Time
    st.markdown("### Hospital Utilization by State Over Time")
    hospital_utilization_df = execute_query(QUERIES["hospital_utilization_by_state_over_time"], conn, [selected_date_str])
    if not hospital_utilization_df.empty:
        plot_hospital_utilization_streamlit(hospital_utilization_df)
    else:
        st.warning("No data available for Hospital Utilization by State Over Time.")

    # Conclusion
    st.markdown("### Conclusion")
    st.write("This concludes the weekly report. The data presented aims to inform decision-making and highlight areas requiring attention.")



def get_available_dates(conn):
    """
    Retrieve the available dates for the last five weeks from the database.

    Args:
        conn (psycopg.Connection): The database connection object.

    Returns:
        list: A list of available dates in the format 'YYYY-MM-DD'.
    """
    query = """
    SELECT DISTINCT collection_week
    FROM weekly_report
    ORDER BY collection_week DESC
    LIMIT 5;
    """
    df = execute_query(query, conn)
    if not df.empty:
        return df['collection_week'].astype(str).tolist()
    else:
        return []


def main():
    """
    Streamlit application entry point.
    """
    st.sidebar.title("HHS Weekly Report Generator")
    st.sidebar.write("Select options below to generate the report.")

    # Database Connection
    try:
        with psycopg.connect(**DB_CONFIG, autocommit=True) as conn:
            st.sidebar.success("Connected to the database.")

            # Get available dates
            available_dates = get_available_dates(conn)

            if available_dates:
                # Let the user select a date from the available options
                selected_date_str = st.sidebar.selectbox("Select Week Ending Date", available_dates)
                selected_date = datetime.strptime(selected_date_str, '%Y-%m-%d').date()
                st.sidebar.write(f"Selected date: {selected_date}")
                
                # Generate the report for the selected date
                generate_report(selected_date, conn)
            else:
                st.sidebar.error("No available dates found in the database.")
    except Exception as e:
        st.sidebar.error(f"Database connection error: {e}")


if __name__ == "__main__":
    main()
