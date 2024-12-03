import pandas as pd
from pathlib import Path
from google.cloud import bigquery
import argparse
from datetime import datetime
import logging


def setup_logging(table_id):
    """Setup logging for BigQuery updates."""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create log filename with timestamp and table name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = table_id.split(".")[-1]
    log_file = log_dir / f"bigquery_update_{table_name}_{timestamp}.log"

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),  # Also print to console
        ],
    )
    return logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload additional species data to BigQuery"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="BigQuery table ID (format: project.dataset.table)",
    )
    parser.add_argument(
        "--backup-bucket",
        help="GCS bucket for table backup (format: bucket-name)",
    )
    return parser.parse_args()


def load_data():
    """Load the additional species data from CSV file."""
    file_path = Path("data/external/2024-10-21_gridVeg_additional_species_SOURCE.csv")
    df = pd.read_csv(file_path)
    return df


def transform_data(df):
    """Transform the data to match BigQuery schema."""
    # Create a copy to avoid modifying original
    df_transformed = df.copy()

    # Rename columns to match BigQuery schema
    df_transformed = df_transformed.rename(
        columns={
            "Survey Data::__kp_Survey": "survey_ID",
            "Survey Data::_kf_Site": "grid_point",
            "Survey Data::SurveyDate": "date",
            "Survey Data::SurveyYear": "year",
            "_kf_Species_serial": "key_plant_species",
        }
    )

    # Convert date format from MM/DD/YYYY to datetime
    df_transformed["date"] = pd.to_datetime(
        df_transformed["date"]
    )  # Keep as datetime object

    # Convert grid_point to integer
    df_transformed["grid_point"] = pd.to_numeric(df_transformed["grid_point"])

    # Handle empty species values and convert to integer
    df_transformed["key_plant_species"] = (
        df_transformed["key_plant_species"]
        .replace("", pd.NA)  # Replace empty strings with NA
        .astype("Int64")  # Convert to nullable integer type
    )

    # Take first 8 characters of UUID for survey_ID
    df_transformed["survey_ID"] = df_transformed["survey_ID"].str[:8]

    # Add validation print
    print("\nColumn types after transformation:")
    print(df_transformed.dtypes)

    return df_transformed


def validate_data(df):
    """Validate the transformed data."""
    print("\nData Validation:")
    print(f"Total records: {len(df)}")
    print(f"Null values in key columns:")
    print(df.isnull().sum())
    print("\nData types:")
    print(df.dtypes)

    return all(
        [
            df["grid_point"].notnull().all(),
            df["date"].notnull().all(),
            df["year"].notnull().all(),
        ]
    )


def upload_to_bigquery(df, table_id, dry_run=True):
    """Upload the transformed data to BigQuery."""
    client = bigquery.Client()

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("survey_ID", "STRING"),
            bigquery.SchemaField("grid_point", "INTEGER"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("key_plant_species", "INTEGER"),
        ],
    )

    if dry_run:
        # Dry run mode - just validate and show statistics
        try:
            # Validate data types match schema
            schema_types = {
                "survey_ID": "object",
                "grid_point": "int64",
                "date": "datetime64[ns]",  # Updated to match datetime type
                "year": "int64",
                "key_plant_species": "Int64",
            }

            # Check if data types match expected schema
            current_types = df.dtypes.to_dict()
            type_matches = all(
                str(current_types[col]) == dtype for col, dtype in schema_types.items()
            )

            if type_matches:
                print(
                    f"\nDry run successful - {len(df)} rows would be uploaded to {table_id}"
                )
                print("Data schema and types are valid")

                # Print some statistics about what would be uploaded
                print("\nUpload Summary (Dry Run):")
                print(f"Total rows: {len(df)}")
                print(f"Unique survey_IDs: {df['survey_ID'].nunique()}")
                print(
                    f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
                )
                print("\nSample of data that would be uploaded:")
                print(df.head())
            else:
                print(
                    "Schema validation failed - data types don't match expected schema"
                )
                print("\nExpected types:")
                for col, dtype in schema_types.items():
                    print(f"{col}: {dtype}")
                print("\nActual types:")
                print(df.dtypes)

        except Exception as e:
            print(f"Dry run validation failed: {e}")
    else:
        # Setup logging for actual upload
        logger = setup_logging(table_id)

        # Actual upload
        try:
            logger.info(f"Starting upload to {table_id}")
            logger.info(f"Total rows to upload: {len(df)}")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete

            logger.info(f"Successfully uploaded {len(df)} rows to BigQuery")
            logger.info("Upload completed successfully")

            print(f"\nSuccessfully uploaded {len(df)} rows to BigQuery")
            print(f"See logs/bigquery_update_*.log for details")

        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            print(f"Error uploading to BigQuery: {e}")


def backup_table(table_id, backup_bucket):
    """Backup BigQuery table to Cloud Storage."""
    client = bigquery.Client()

    # Create timestamp for backup file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = table_id.split(".")[-1]
    backup_path = f"gs://{backup_bucket}/backups/{table_name}/{timestamp}/backup_*.csv"

    # Configure the extract job
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV

    try:
        # Start extract job
        extract_job = client.extract_table(table_id, backup_path, job_config=job_config)
        extract_job.result()  # Wait for job to complete

        print(f"\nBackup created successfully: {backup_path}")
        return True
    except Exception as e:
        print(f"Error creating backup: {e}")
        return False


def main():
    # Parse command line arguments
    args = parse_args()

    # Load the data
    print("Loading data...")
    df = load_data()

    # Transform the data
    print("Transforming data...")
    df_transformed = transform_data(df)

    # Validate the transformed data
    print("Validating transformed data...")
    if validate_data(df_transformed):
        print("\nData validation passed!")

        # Preview the transformed data
        print("\nFirst few rows of transformed data:")
        print(df_transformed.head())

        # Ask if this is a dry run
        run_type = input("\nRun in test mode (dry run)? (y/n): ")
        if run_type.lower() == "y":
            # Dry run mode
            upload_to_bigquery(df_transformed, args.table, dry_run=True)
        else:
            # Create backup if bucket specified
            if args.backup_bucket:
                print(f"\nCreating backup in bucket: {args.backup_bucket}")
                if not backup_table(args.table, args.backup_bucket):
                    response = input(
                        "\nBackup failed. Continue with upload anyway? (y/n): "
                    )
                    if response.lower() != "y":
                        print("Upload cancelled.")
                        return

            # Ask for confirmation before real upload
            response = input("\nProceed with actual upload to BigQuery? (y/n): ")
            if response.lower() == "y":
                upload_to_bigquery(df_transformed, args.table, dry_run=False)
            else:
                print("Upload cancelled.")
    else:
        print("Data validation failed. Please check the data before uploading.")


if __name__ == "__main__":
    main()
