import pandas as pd
from pathlib import Path
from google.cloud import bigquery
import argparse
from datetime import datetime
import logging


def setup_logging(table_id):
    """Setup logging for BigQuery updates."""
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)

    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"bigquery_update_gridVeg_image_metadata_{timestamp}.log"

    # Configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers = []

    # Add handlers
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def load_data():
    """Load the image metadata from CSV file."""
    file_path = (
        Path(__file__).parents[2]
        / "data/external/2024-10-21_gridVeg_ref_image_metadata_SOURCE.csv"
    )
    df = pd.read_csv(file_path)
    return df


def transform_data(df):
    """Transform the data to match BigQuery schema."""
    df_transformed = df.copy()

    # Rename columns
    df_transformed = df_transformed.rename(
        columns={
            "__kp_Photos": "image_ID",
            "Survey Data::__kp_Survey": "survey_ID",
            "Survey Data::SurveyDate": "date",
            "Survey Data::SurveyYear": "year",
            "Survey Data::_kf_Site": "grid_point",
            "Direction": "image_direction",
        }
    )

    # Add image_url column (placeholder - you'll need to specify the actual URL pattern)
    df_transformed["image_url"] = (
        None  # Replace with actual URL construction if available
    )

    # Convert date format
    df_transformed["date"] = pd.to_datetime(df_transformed["date"])

    # Convert numeric fields
    df_transformed["year"] = pd.to_numeric(df_transformed["year"])
    df_transformed["grid_point"] = pd.to_numeric(df_transformed["grid_point"])

    # Reorder columns to match schema
    columns_order = [
        "image_ID",
        "image_url",
        "survey_ID",
        "date",
        "year",
        "grid_point",
        "image_direction",
    ]
    df_transformed = df_transformed[columns_order]

    return df_transformed


def validate_data(df, logger):
    """Validate the transformed data."""
    validation_output = []
    validation_output.append("\nValidating Image Metadata:")
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
            df["image_ID"].notnull().all(),
            df["survey_ID"].notnull().all(),
            df["date"].notnull().all(),
            df["year"].notnull().all(),
            df["grid_point"].notnull().all(),
            df["image_direction"].notnull().all(),
        ]
    )

    return required_valid


def upload_to_bigquery(df, table_id, dry_run=True, logger=None):
    """Upload the transformed data to BigQuery."""
    client = bigquery.Client()

    # Define schema
    schema = [
        bigquery.SchemaField("image_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("survey_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("grid_point", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("image_direction", "STRING", mode="NULLABLE"),
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
                f"Unique image IDs: {df['image_ID'].nunique()}",
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
    parser = argparse.ArgumentParser(description="Upload image metadata to BigQuery")
    parser.add_argument(
        "--table-id",
        required=True,
        help="BigQuery table ID (format: project.dataset.table)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview the upload without performing it",
    )
    return parser.parse_args()


def main():
    # Parse command line arguments
    args = parse_args()

    # Setup logging
    logger = setup_logging(args.table_id)
    logger.info("Starting image metadata processing")
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

    # Upload to BigQuery
    upload_to_bigquery(
        df_transformed, args.table_id, dry_run=args.dry_run, logger=logger
    )


if __name__ == "__main__":
    main()
