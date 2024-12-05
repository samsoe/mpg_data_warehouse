"""Fix date transformation issues in gridVeg_additional_species table.

The script corrects dates that were incorrectly transformed from DD-MM-YY format
by having the day (DD) prefixed with '20' and treated as the year.
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import argparse
import logging
import sys
import os

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'logs/fix_gridveg_dates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(sys.stdout),
    ],
)


def connect_to_bigquery():
    """Create a BigQuery client"""
    return bigquery.Client()


def create_backup(client, project_id, dataset_id, table_id, bucket_name):
    """Create a backup of the table in Google Cloud Storage

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: Source table ID
        bucket_name: GCS bucket for backup
    """
    if not bucket_name:
        raise ValueError("bucket_name is required for backup")

    source_table = f"{project_id}.{dataset_id}.{table_id}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination_uri = (
        f"gs://{bucket_name}/backups/{dataset_id}/{table_id}/{timestamp}/*.csv"
    )

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV

    logging.info(f"Creating backup in Cloud Storage: {destination_uri}")
    extract_job = client.extract_table(
        table_ref, destination_uri, job_config=job_config
    )
    extract_job.result()  # Wait for job to complete

    # Verify the backup exists
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    prefix = f"backups/{dataset_id}/{table_id}/{timestamp}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        raise ValueError("Backup verification failed: No files found in GCS")

    logging.info("Cloud Storage backup completed successfully")
    logging.info(f"Backup location: {destination_uri}")

    return destination_uri


def validate_dates(client, table_ref):
    """Validate that no dates are in the future after the fix"""
    query = f"""
    SELECT COUNT(*) as future_dates
    FROM {table_ref}
    WHERE date > CURRENT_DATE()
    """

    result = client.query(query).to_dataframe()
    future_dates = result["future_dates"].iloc[0]

    if future_dates > 0:
        raise ValueError(f"Validation failed: Found {future_dates} dates in the future")

    logging.info("Date validation passed: No future dates found")


def fix_dates(client, project_id, dataset_id, table_id, bucket_name, dry_run=True):
    """Fix the incorrectly transformed dates using metadata as source of truth"""
    source_table = f"{project_id}.{dataset_id}.{table_id}"

    # First, preview the changes
    preview_query = """
    WITH to_fix AS (
        SELECT 
            a.survey_ID,
            a.date as incorrect_date,
            m.date as correct_date,
            a.year as incorrect_year,
            EXTRACT(YEAR FROM m.date) as correct_year,
            COUNT(*) as affected_rows
        FROM 
            `{project}.{dataset}.{table}` a
        JOIN 
            `{project}.{dataset}.gridVeg_survey_metadata` m
        ON 
            a.survey_ID = m.survey_ID
        WHERE 
            a.date > '2024-12-31'
        GROUP BY 
            a.survey_ID, a.date, m.date, a.year
    )
    SELECT 
        *,
        MIN(incorrect_date) OVER () as min_incorrect_date,
        MAX(incorrect_date) OVER () as max_incorrect_date,
        MIN(correct_date) OVER () as min_correct_date,
        MAX(correct_date) OVER () as max_correct_date
    FROM to_fix
    ORDER BY incorrect_date
    """.format(
        project=project_id, dataset=dataset_id, table=table_id
    )

    preview_df = client.query(preview_query).to_dataframe()

    logging.info(f"Preview of changes to be made:")
    logging.info(f"Total records to update: {preview_df['affected_rows'].sum()}")
    logging.info(f"\nDate ranges:")
    logging.info(
        f"  Incorrect dates: {preview_df['min_incorrect_date'].iloc[0]} to {preview_df['max_incorrect_date'].iloc[0]}"
    )
    logging.info(
        f"  Correct dates: {preview_df['min_correct_date'].iloc[0]} to {preview_df['max_correct_date'].iloc[0]}"
    )
    logging.info("\nSample of changes (showing date and year corrections):")

    # Format the preview to clearly show both date and year changes
    for _, row in preview_df.head().iterrows():
        logging.info(
            f"Survey ID: {row['survey_ID']}\n"
            f"  Date: {row['incorrect_date']} -> {row['correct_date']}\n"
            f"  Year: {row['incorrect_year']} -> {row['correct_year']}\n"
            f"  Affected rows: {row['affected_rows']}\n"
        )

    if dry_run:
        logging.info("Dry run completed. No changes made.")
        return

    # Create backup
    backup_uri = create_backup(client, project_id, dataset_id, table_id, bucket_name)

    # Update both date and year columns
    update_query = """
    UPDATE `{project}.{dataset}.{table}` a
    SET 
        date = m.date,
        year = EXTRACT(YEAR FROM m.date)
    FROM `{project}.{dataset}.gridVeg_survey_metadata` m
    WHERE a.survey_ID = m.survey_ID
    AND a.date > '2024-12-31'
    """

    logging.info("Executing update...")
    job = client.query(update_query)
    job.result()

    # Validate the changes
    validate_dates(client, source_table)

    # Additional validation for year column
    year_validation_query = """
    SELECT COUNT(*) as mismatched_years
    FROM `{project}.{dataset}.{table}`
    WHERE EXTRACT(YEAR FROM date) != year
    """.format(
        project=project_id, dataset=dataset_id, table=table_id
    )

    year_validation = client.query(year_validation_query).to_dataframe()
    if year_validation["mismatched_years"].iloc[0] > 0:
        raise ValueError(
            f"Year validation failed: Found {year_validation['mismatched_years'].iloc[0]} records where year doesn't match date"
        )

    logging.info("Update completed successfully")
    logging.info(f"Backup available at: {backup_uri}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix date transformation issues in gridVeg_additional_species"
    )
    parser.add_argument("--project-id", required=True, help="BigQuery project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")
    parser.add_argument("--table-id", required=True, help="BigQuery table ID")
    parser.add_argument(
        "--bucket-name", required=True, help="GCS bucket name for backup"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview changes without making them"
    )

    args = parser.parse_args()

    client = connect_to_bigquery()

    try:
        fix_dates(
            client,
            args.project_id,
            args.dataset_id,
            args.table_id,
            args.bucket_name,
            args.dry_run,
        )
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
