import pandas as pd
from pathlib import Path
from google.cloud import bigquery
import argparse
from datetime import datetime
import logging


def setup_logging(table_id):
    """Setup logging for BigQuery updates."""
    log_dir = Path(__file__).parents[2] / "logs" / "survey_metadata"
    log_dir.mkdir(exist_ok=True, parents=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"bigquery_update_gridVeg_survey_metadata_{timestamp}.log"

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers = []

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def load_data():
    """Load the survey metadata from CSV file."""
    file_path = (
        Path(__file__).parents[2]
        / "data/external/2024-10-21_gridVeg_survey_metadata_SOURCE.csv"
    )
    df = pd.read_csv(file_path)
    return df


def transform_data(df):
    """Transform the data to match BigQuery schema."""
    df_transformed = df.copy()

    # Rename columns
    df_transformed = df_transformed.rename(
        columns={
            "__kp_Survey": "survey_ID",
            "_kf_Site": "grid_point",
            "SurveyYear": "year",
            "SurveyDate": "date",
            "Surveyor1": "surveyor",
        }
    )

    # Convert date format
    df_transformed["date"] = pd.to_datetime(df_transformed["date"])

    # Convert numeric fields
    df_transformed["year"] = pd.to_numeric(df_transformed["year"])
    df_transformed["grid_point"] = pd.to_numeric(df_transformed["grid_point"])

    # Add survey_sequence column (null for now, can be updated if needed)
    df_transformed["survey_sequence"] = None

    # Reorder columns to match schema
    columns_order = [
        "survey_ID",
        "grid_point",
        "year",
        "date",
        "survey_sequence",
        "surveyor",
    ]
    df_transformed = df_transformed[columns_order]

    return df_transformed


def validate_data(df, logger):
    """Validate the transformed data."""
    validation_output = []
    validation_output.append("\nValidating Survey Metadata:")
    validation_output.append(f"Total records: {len(df)}")

    # Check for null values
    validation_output.append("\nNull values in key columns:")
    null_counts = df.isnull().sum()
    validation_output.append(str(null_counts))

    # Check data types
    validation_output.append("\nData types:")
    validation_output.append(str(df.dtypes))

    # Log validation output
    for line in validation_output:
        logger.info(line)

    # Validate required fields
    required_valid = all(
        [
            df["survey_ID"].notnull().all(),
            df["grid_point"].notnull().all(),
            df["year"].notnull().all(),
            df["date"].notnull().all(),
            df["surveyor"].notnull().all(),
        ]
    )

    return required_valid


def upload_to_bigquery(df, table_id, dry_run=True, logger=None):
    """Upload the transformed data to BigQuery."""
    client = bigquery.Client()

    # Define schema
    schema = [
        bigquery.SchemaField("survey_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("grid_point", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("survey_sequence", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("surveyor", "STRING", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    if dry_run:
        try:
            # Print upload summary
            summary = [
                f"\nDry run successful - {len(df)} rows would be uploaded to {table_id}",
                f"\nUpload Summary (Dry Run):",
                f"Total rows: {len(df)}",
                f"Unique survey IDs: {df['survey_ID'].nunique()}",
                f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}",
                f"Unique grid points: {df['grid_point'].nunique()}",
                "\nSample of data that would be uploaded:",
                str(df.head()),
            ]

            for line in summary:
                print(line)
                if logger:
                    logger.info(line)

        except Exception as e:
            error_msg = f"Dry run validation failed: {e}"
            print(error_msg)
            if logger:
                logger.error(error_msg)
    else:
        try:
            # Convert DataFrame to records
            records = df.to_dict("records")

            # Handle date conversion
            for record in records:
                if isinstance(record["date"], pd.Timestamp):
                    record["date"] = record["date"].strftime("%Y-%m-%d")

            job = client.load_table_from_json(records, table_id, job_config=job_config)
            job.result()

            logger.info(f"Successfully uploaded {len(df)} rows to BigQuery")
            print(f"\nSuccessfully uploaded {len(df)} rows to BigQuery")

        except Exception as e:
            if logger:
                logger.error(f"Error uploading to BigQuery: {e}")
            print(f"Error uploading to BigQuery: {e}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Upload survey metadata to BigQuery")
    parser.add_argument(
        "--table-id",
        required=True,
        help="BigQuery table ID (format: project.dataset.table)",
    )
    parser.add_argument(
        "--backup-bucket",
        help="GCS bucket for table backup (format: bucket-name)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview the upload without performing it",
    )
    return parser.parse_args()


def backup_table(table_id, backup_bucket):
    """Backup BigQuery table to Cloud Storage."""
    client = bigquery.Client()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = table_id.split(".")[-1]
    backup_path = f"gs://{backup_bucket}/backups/{table_name}/survey_metadata/{timestamp}/backup_*.csv"

    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV

    try:
        extract_job = client.extract_table(table_id, backup_path, job_config=job_config)
        extract_job.result()

        print(f"\nBackup created successfully: {backup_path}")
        return True
    except Exception as e:
        print(f"Error creating backup: {e}")
        return False


def main():
    # Parse command line arguments
    args = parse_args()

    # Setup logging
    logger = setup_logging(args.table_id)
    logger.info("Starting survey metadata processing")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    # Load and transform data
    print("Loading data...")
    df = load_data()

    print("\nTransforming data...")
    df_transformed = transform_data(df)

    # Validate data
    if not validate_data(df_transformed, logger):
        print("Data validation failed.")
        return

    print("\nData validation passed!")
    logger.info("Data validation passed!")

    # Handle backup and upload
    if args.dry_run:
        upload_to_bigquery(df_transformed, args.table_id, dry_run=True, logger=logger)
    else:
        if args.backup_bucket:
            print(f"\nCreating table backup...")
            if not backup_table(args.table_id, args.backup_bucket):
                print("Backup failed. Aborting upload.")
                return

        upload_to_bigquery(df_transformed, args.table_id, dry_run=False, logger=logger)


if __name__ == "__main__":
    main()
