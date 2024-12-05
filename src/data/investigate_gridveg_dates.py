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

    # Identify dates affected by the transformation error (after 2024)
    future_dates = df[df["date"].dt.year > 2024]
    if not future_dates.empty:
        print("\nDates affected by transformation error (after 2024):")
        print(future_dates)
        print(f"\nNumber of records with affected dates: {future_dates['count'].sum()}")

    return df


def analyze_with_metadata(client):
    """Cross reference dates with survey metadata and look for patterns"""
    query = """
    WITH additional_species AS (
        SELECT 
            a.survey_ID,
            a.date as incorrect_date,
            m.date as correct_date,
            FORMAT_DATE('%d-%m-%y', m.date) as original_format,
            FORMAT_DATE('%Y-%m-%d', a.date) as transformed_date
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species` a
        LEFT JOIN 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata` m
        ON a.survey_ID = m.survey_ID
        WHERE a.date > '2024-12-31'
    )
    SELECT 
        survey_ID,
        incorrect_date,
        correct_date,
        original_format,
        transformed_date,
        CASE 
            WHEN CAST(FORMAT_DATE('%d', correct_date) AS INT64) = 
                 CAST(FORMAT_DATE('%Y', incorrect_date) AS INT64) - 2000
            THEN 'Confirms Pattern'
            ELSE 'Pattern Mismatch'
        END as pattern_check
    FROM additional_species
    ORDER BY incorrect_date
    LIMIT 10
    """

    df = client.query(query).to_dataframe()
    if not df.empty:
        print("\nDate Transformation Pattern Analysis:")
        print("Showing how DD-MM-YY was incorrectly transformed to YYYY-MM-DD")
        print("\nExample records:")
        for _, row in df.iterrows():
            print(f"\nSurvey ID: {row['survey_ID']}")
            print(f"Original format (DD-MM-YY): {row['original_format']}")
            print(f"Incorrect transform: {row['transformed_date']}")
            print(f"Correct date: {row['correct_date'].strftime('%Y-%m-%d')}")
            print(f"Pattern check: {row['pattern_check']}")

    return df


def analyze_metadata_coverage(client):
    """Analyze how many additional_species records can be matched with metadata"""
    query = """
    WITH additional_species_dates AS (
        SELECT DISTINCT
            survey_ID,
            date as species_date
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
    )
    SELECT
        COUNT(*) as total_species_records,
        COUNTIF(m.date IS NOT NULL) as matched_with_metadata,
        COUNTIF(m.date IS NULL) as unmatched_records,
        COUNTIF(a.species_date > '2024-12-31' AND m.date IS NULL) as future_dates_without_metadata
    FROM additional_species_dates a
    LEFT JOIN 
        `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata` m
    ON a.survey_ID = m.survey_ID
    """

    df = client.query(query).to_dataframe()
    if not df.empty:
        print("\nMetadata Coverage Analysis:")
        print(
            f"Total unique survey_ID/date combinations: {df['total_species_records'].iloc[0]}"
        )
        print(f"Records with matching metadata: {df['matched_with_metadata'].iloc[0]}")
        print(f"Records without metadata: {df['unmatched_records'].iloc[0]}")
        print(
            f"Future dates without metadata: {df['future_dates_without_metadata'].iloc[0]}"
        )

        # Calculate percentages
        total = df["total_species_records"].iloc[0]
        matched = df["matched_with_metadata"].iloc[0]
        print(f"\nMetadata coverage: {(matched/total*100):.1f}%")

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


def main():
    client = connect_to_bigquery()
    table_id = "mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species"

    # Get schema
    get_table_schema(client, table_id)

    # Analyze dates
    dates_df = analyze_dates(client)

    # Analyze dates with metadata
    metadata_analysis = analyze_with_metadata(client)

    # Analyze metadata coverage
    coverage_analysis = analyze_metadata_coverage(client)

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
