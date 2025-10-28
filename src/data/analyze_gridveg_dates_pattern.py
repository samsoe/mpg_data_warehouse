"""Compare dates between gridVeg tables to validate survey dates.

This script analyzes and compares dates across different gridVeg tables to validate
survey dates and identify any discrepancies.
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import logging
import os

# Set up logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'logs/analyze_gridveg_dates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(),
    ],
)

def connect_to_bigquery():
    """Create a BigQuery client"""
    return bigquery.Client()

def compare_dates_across_tables(client):
    """Compare dates between additional_species and other gridVeg tables"""
    query = """
    WITH additional_species AS (
        SELECT DISTINCT
            __kp_Survey as survey_ID,
            date as species_date,
            year as species_year
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
    ),
    metadata AS (
        SELECT DISTINCT
            __kp_Survey as survey_ID,
            date as metadata_date
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata`
    ),
    point_intercept AS (
        SELECT DISTINCT
            __kp_Survey as survey_ID,
            date as intercept_date
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_point_intercept_vegetation`
    ),
    ground_cover AS (
        SELECT DISTINCT
            __kp_Survey as survey_ID,
            date as ground_date
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_ground_cover_metadata`
    )
    SELECT 
        a.survey_ID,
        a.species_date,
        m.metadata_date,
        p.intercept_date,
        g.ground_date,
        CASE 
            WHEN a.species_date > '2024-12-31' THEN 'Future Date'
            WHEN a.species_date != m.metadata_date THEN 'Date Mismatch'
            ELSE 'Match'
        END as status,
        COUNT(*) OVER(PARTITION BY 
            CASE 
                WHEN a.species_date > '2024-12-31' THEN 'Future Date'
                WHEN a.species_date != m.metadata_date THEN 'Date Mismatch'
                ELSE 'Match'
            END
        ) as category_count
    FROM additional_species a
    LEFT JOIN metadata m ON a.survey_ID = m.survey_ID
    LEFT JOIN point_intercept p ON a.survey_ID = p.survey_ID
    LEFT JOIN ground_cover g ON a.survey_ID = g.survey_ID
    ORDER BY 
        CASE 
            WHEN a.species_date > '2024-12-31' THEN 1
            WHEN a.species_date != m.metadata_date THEN 2
            ELSE 3
        END,
        a.species_date
    """
    
    return client.query(query).to_dataframe()

def analyze_results(df):
    """Analyze and display the comparison results"""
    # Overall statistics
    total_records = len(df)
    future_dates = df[df['status'] == 'Future Date']
    mismatches = df[df['status'] == 'Date Mismatch']
    matches = df[df['status'] == 'Match']
    
    print("\n=== Date Comparison Analysis ===")
    print(f"\nTotal Records Analyzed: {total_records:,}")
    print(f"Records with Future Dates: {len(future_dates):,} ({len(future_dates)/total_records*100:.1f}%)")
    print(f"Records with Date Mismatches: {len(mismatches):,} ({len(mismatches)/total_records*100:.1f}%)")
    print(f"Records with Matching Dates: {len(matches):,} ({len(matches)/total_records*100:.1f}%)")
    
    if not future_dates.empty:
        print("\n=== Sample of Future Dates ===")
        sample = future_dates.head()
        for _, row in sample.iterrows():
            print(f"\nSurvey ID: {row['survey_ID']}")
            print(f"  Additional Species Date: {row['species_date']}")
            print(f"  Metadata Date: {row['metadata_date']}")
            print(f"  Point Intercept Date: {row['intercept_date']}")
            print(f"  Ground Cover Date: {row['ground_date']}")
    
    if not mismatches.empty:
        print("\n=== Sample of Date Mismatches ===")
        sample = mismatches.head()
        for _, row in sample.iterrows():
            print(f"\nSurvey ID: {row['survey_ID']}")
            print(f"  Additional Species Date: {row['species_date']}")
            print(f"  Metadata Date: {row['metadata_date']}")
            print(f"  Point Intercept Date: {row['intercept_date']}")
            print(f"  Ground Cover Date: {row['ground_date']}")

def main():
    client = connect_to_bigquery()
    logging.info("Starting date comparison analysis across gridVeg tables...")
    
    # Compare dates across tables
    results = compare_dates_across_tables(client)
    logging.info("Completed date comparison query")
    
    # Analyze and display results
    analyze_results(results)
    logging.info("Analysis complete")

if __name__ == "__main__":
    main() 