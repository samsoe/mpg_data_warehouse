from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def connect_to_bigquery():
    """Create a BigQuery client"""
    return bigquery.Client()


def get_table_schema(client, table_id):
    """Get and print the schema of a specified table"""
    table = client.get_table(table_id)
    print("\nTable Schema:")
    for field in table.schema:
        print(f"{field.name}: {field.field_type} ({field.mode})")


def analyze_dates(client):
    """Analyze dates in the gridVeg_additional_species table"""
    query = """
    SELECT 
        date,
        COUNT(*) as count
    FROM 
        `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
    GROUP BY date
    ORDER BY date
    """

    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])

    print("\nDate Analysis:")
    print(f"Total unique dates: {len(df)}")
    print(f"Date range: from {df['date'].min()} to {df['date'].max()}")

    # Identify suspicious dates (beyond 2024)
    suspicious_dates = df[df["date"].dt.year > 2024]
    if not suspicious_dates.empty:
        print("\nSuspicious dates found (after 2024):")
        print(suspicious_dates)
        print(
            f"\nNumber of records with suspicious dates: {suspicious_dates['count'].sum()}"
        )

    return df


def check_related_tables(client):
    """Query related tables that might have corresponding date information"""
    query = """
    SELECT 
        table_name
    FROM 
        mpg-data-warehouse.vegetation_point_intercept_gridVeg.INFORMATION_SCHEMA.TABLES
    """

    tables_df = client.query(query).to_dataframe()
    print("\nRelated tables in the dataset:")
    for table in tables_df["table_name"]:
        print(f"- {table}")


def analyze_with_metadata(client):
    """Cross reference dates with survey metadata and look for patterns"""
    # First get the schema of survey_metadata
    metadata_table = client.get_table(
        "mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata"
    )
    print("\nSurvey Metadata Schema:")
    for field in metadata_table.schema:
        print(f"{field.name}: {field.field_type}")

    query = """
    WITH additional_species AS (
        SELECT 
            survey_ID,
            date as species_date,
            COUNT(*) as record_count
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
        WHERE 
            date > '2024-12-31'
        GROUP BY survey_ID, date
    )
    SELECT 
        a.survey_ID,
        a.species_date,
        m.date as metadata_date,
        a.record_count
    FROM additional_species a
    LEFT JOIN 
        `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata` m
    ON a.survey_ID = m.survey_ID
    ORDER BY a.species_date
    """

    df = client.query(query).to_dataframe()

    if not df.empty:
        print("\nAnalysis of suspicious dates with metadata:")
        print(f"Total affected surveys: {df['survey_ID'].nunique()}")
        print("\nSample of affected records:")
        print(df.head())

        # Check for date mismatches
        df["date_mismatch"] = df["species_date"] != df["metadata_date"]
        mismatches = df[df["date_mismatch"]]
        if not mismatches.empty:
            print("\nFound date mismatches between tables:")
            print(mismatches[["survey_ID", "species_date", "metadata_date"]])

    return df


def analyze_date_patterns(client):
    """Analyze the patterns between metadata and species dates"""
    query = """
    WITH additional_species AS (
        SELECT 
            survey_ID,
            date as species_date,
            COUNT(*) as record_count
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
        WHERE 
            date > '2024-12-31'
        GROUP BY survey_ID, date
    )
    SELECT 
        a.survey_ID,
        a.species_date,
        m.date as metadata_date,
        DATE_DIFF(a.species_date, m.date, YEAR) as year_difference,
        a.record_count
    FROM additional_species a
    LEFT JOIN 
        `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata` m
    ON a.survey_ID = m.survey_ID
    ORDER BY year_difference, a.species_date
    """

    df = client.query(query).to_dataframe()
    if not df.empty:
        print("\nDate Difference Analysis:")
        year_diff_counts = df["year_difference"].value_counts()
        print("\nYear differences between species and metadata dates:")
        print(year_diff_counts)

    return df


def main():
    client = connect_to_bigquery()
    table_id = "mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species"

    # Get schema
    get_table_schema(client, table_id)

    # Analyze dates
    dates_df = analyze_dates(client)

    # Analyze dates with metadata
    metadata_analysis = analyze_with_metadata(client)

    # Check related tables
    check_related_tables(client)

    # Optional: Create a histogram of dates
    plt.figure(figsize=(12, 6))
    plt.hist(dates_df["date"], bins=50)
    plt.title("Distribution of Dates in gridVeg_additional_species")
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("date_distribution.png")


if __name__ == "__main__":
    main()
