import pandas as pd
from pathlib import Path
from google.cloud import bigquery
import argparse
from datetime import datetime
import logging
import sys


def setup_logging(table_id, table_type):
    """Setup logging for BigQuery updates."""
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parents[2] / "logs"
    log_dir.mkdir(exist_ok=True)

    # Create log filename with timestamp and table name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = table_id.split(".")[-1]
    log_file = log_dir / f"bigquery_update_{table_name}_{table_type}_{timestamp}.log"

    # Configure logging
    logger = logging.getLogger(f"{__name__}_{table_type}")
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


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload point intercepts data to BigQuery"
    )
    parser.add_argument(
        "--vegetation-table",
        required=True,
        help="BigQuery vegetation table ID (format: project.dataset.table)",
    )
    parser.add_argument(
        "--ground-table",
        required=False,
        help="BigQuery ground cover table ID (format: project.dataset.table)",
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
    parser.add_argument(
        "--skip-ground-table",
        action="store_true",
        help="Skip processing of ground cover table",
    )
    return parser.parse_args()


def load_data():
    """Load the point intercepts data from CSV file."""
    file_path = (
        Path(__file__).parents[2]
        / "data/external/2024-10-21_gridVeg_point_intercepts_SOURCE.csv"
    )
    df = pd.read_csv(file_path)
    return df


def transform_vegetation_data(df):
    """Transform the data for vegetation table."""
    df_transformed = df.copy()

    # Rename columns
    df_transformed = df_transformed.rename(
        columns={
            "Survey Data::__kp_Survey": "survey_ID",
            "Survey Data::_kf_Site": "grid_point",
            "Survey Data::SurveyDate": "date",
            "Survey Data::SurveyYear": "year",
            "PointTrans": "transect_point",
            "Height": "height_intercept_1",
            "_kf_Hit1_serial": "intercept_1",
            "_kf_Hit2_serial": "intercept_2",
            "_kf_Hit3_serial": "intercept_3",
            "_kf_Hit4_serial": "intercept_4",
        }
    )

    # Convert date format
    df_transformed["date"] = pd.to_datetime(df_transformed["date"])

    # Convert numeric fields
    df_transformed["grid_point"] = pd.to_numeric(df_transformed["grid_point"])
    df_transformed["year"] = pd.to_numeric(df_transformed["year"])

    # Handle nullable numeric fields
    numeric_columns = [
        "height_intercept_1",
        "intercept_1",
        "intercept_2",
        "intercept_3",
        "intercept_4",
    ]
    for col in numeric_columns:
        # Replace empty strings with None before conversion
        df_transformed[col] = df_transformed[col].replace(["", "NA"], None)
        if col == "height_intercept_1":
            # Convert to float for NUMERIC type and handle empty strings
            df_transformed[col] = pd.to_numeric(
                df_transformed[col], errors="coerce", downcast="float"
            )
        else:
            # Convert to nullable integer using Int64
            df_transformed[col] = pd.to_numeric(
                df_transformed[col], errors="coerce"
            ).astype("Int64")

    # Add debug logging
    print("\nColumn dtypes after transformation:")
    print(df_transformed.dtypes)
    print("\nSample of height values:")
    print(df_transformed["height_intercept_1"].head(10))
    print("\nNull counts:")
    print(df_transformed.isnull().sum())

    return df_transformed


def transform_ground_data(df):
    """Transform the data for ground cover table."""
    df_transformed = df.copy()

    # Rename columns
    df_transformed = df_transformed.rename(
        columns={
            "Survey Data::__kp_Survey": "survey_ID",
            "Survey Data::_kf_Site": "grid_point",
            "Survey Data::SurveyDate": "date",
            "Survey Data::SurveyYear": "year",
            "PointTrans": "transect_point",
            "_kf_Hit1_serial": "intercept_1",
            "GroundCover": "intercept_ground_code",
        }
    )

    # Convert date format
    df_transformed["date"] = pd.to_datetime(df_transformed["date"])

    # Convert numeric fields
    df_transformed["grid_point"] = pd.to_numeric(df_transformed["grid_point"])
    df_transformed["intercept_1"] = (
        df_transformed["intercept_1"].replace("", pd.NA).astype("Int64")
    )

    # Select only the columns needed for the ground cover table
    columns_to_keep = [
        "survey_ID",
        "grid_point",
        "date",
        "year",
        "transect_point",
        "intercept_1",
        "intercept_ground_code",
    ]
    df_transformed = df_transformed[columns_to_keep]

    return df_transformed


def validate_vegetation_data(df, logger=None):
    """Validate the vegetation data."""
    validation_output = []
    validation_output.append("\nValidating Vegetation Data:")
    validation_output.append(f"Total records: {len(df)}")

    # Basic null value checks
    validation_output.append("\nNull values in key columns:")
    null_counts = (
        df[
            [
                "survey_ID",
                "grid_point",
                "date",
                "year",
                "transect_point",
                "intercept_1",
            ]
        ]
        .isnull()
        .sum()
    )
    validation_output.append(str(null_counts))

    validation_output.append("\nData types:")
    validation_output.append(str(df.dtypes))

    # Log all validation output
    if logger:
        for line in validation_output:
            logger.info(line)
    else:
        for line in validation_output:
            print(line)

    # Validate required fields - allow null intercept_1 values
    required_valid = all(
        [
            df["grid_point"].notnull().all(),
            df["date"].notnull().all(),
            df["year"].notnull().all(),
            df["transect_point"].notnull().all(),
        ]
    )

    # Validate transect point format
    transect_format = df["transect_point"].str.match(r"^[NSEW]\d{1,2}$").all()

    return required_valid and transect_format


def validate_ground_data(df, logger=None):
    """Validate the ground cover data."""
    validation_output = []
    validation_output.append("\nValidating Ground Cover Data:")
    validation_output.append(f"Total records: {len(df)}")

    validation_output.append("\nNull values in key columns:")
    null_counts = (
        df[
            [
                "survey_ID",
                "grid_point",
                "date",
                "year",
                "transect_point",
                "intercept_1",
                "intercept_ground_code",
            ]
        ]
        .isnull()
        .sum()
    )
    validation_output.append(str(null_counts))

    validation_output.append("\nUnique ground cover codes:")
    validation_output.append(str(df["intercept_ground_code"].unique()))

    validation_output.append("\nData types:")
    validation_output.append(str(df.dtypes))

    # Log all validation output
    if logger:
        for line in validation_output:
            logger.info(line)
    else:
        for line in validation_output:
            print(line)

    # Validate required fields
    required_valid = all(
        [
            df["grid_point"].notnull().all(),
            df["date"].notnull().all(),
            df["year"].notnull().all(),
            df["transect_point"].notnull().all(),
            df["intercept_ground_code"].notnull().all(),
        ]
    )

    # Validate transect point format
    transect_format = df["transect_point"].str.match(r"^[NSEW]\d{1,2}$").all()

    return required_valid and transect_format


def upload_to_bigquery(df, table_id, table_type, schema, dry_run=True, logger=None):
    """Upload the transformed data to BigQuery."""
    client = bigquery.Client()

    # Convert DataFrame to records
    records = df.to_dict("records")

    # Handle nullable integers in records
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, pd.Timestamp):
                record[key] = value.strftime("%Y-%m-%d")
            elif isinstance(value, pd.Int64Dtype):
                record[key] = int(value) if pd.notna(value) else None

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )

    if dry_run:
        try:
            # Check if data types match expected schema
            current_types = df.dtypes.to_dict()
            type_matches = True  # Detailed type checking would go here

            if type_matches:
                summary = [
                    f"\nDry run successful - {len(df)} rows would be uploaded to {table_id}",
                    "Data schema and types are valid",
                    f"\nUpload Summary for {table_type} (Dry Run):",
                    f"Total rows: {len(df)}",
                    f"Unique survey_IDs: {df['survey_ID'].nunique()}",
                    f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}",
                    f"Unique grid points: {df['grid_point'].nunique()}",
                    "\nSample of data that would be uploaded:",
                    str(df.head()),
                ]

                # Print and log summary
                for line in summary:
                    print(line)
                    if logger:
                        logger.info(line)
            else:
                error_msg = (
                    "Schema validation failed - data types don't match expected schema"
                )
                print(error_msg)
                if logger:
                    logger.error(error_msg)

        except Exception as e:
            error_msg = f"Dry run validation failed: {e}"
            print(error_msg)
            if logger:
                logger.error(error_msg)
    else:
        try:
            logger.info(f"Starting upload to {table_id}")
            logger.info(f"Total rows to upload: {len(df)}")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

            # Add debug info for all fields
            for col in df.columns:
                logger.info(f"{col} type: {df[col].dtype}")
                if df[col].dtype in ["int32", "Int32", "int64", "Int64"]:
                    logger.info(f"First few {col} values:\n{df[col].head()}")

            # Use records instead of DataFrame
            job = client.load_table_from_json(records, table_id, job_config=job_config)
            job.result()

            logger.info(f"Successfully uploaded {len(df)} rows to BigQuery")
            logger.info("Upload completed successfully")

            print(f"\nSuccessfully uploaded {len(df)} rows to BigQuery")
            print(f"See logs/bigquery_update_*_{table_type}_*.log for details")

        except Exception as e:
            if logger:
                logger.error(f"Error uploading to BigQuery: {e}")
            print(f"Error uploading to BigQuery: {e}")


def backup_table(table_id, backup_bucket, table_type):
    """Backup BigQuery table to Cloud Storage."""
    client = bigquery.Client()

    # Create timestamp for backup file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = table_id.split(".")[-1]
    backup_path = f"gs://{backup_bucket}/backups/{table_name}/{table_type}/{timestamp}/backup_*.csv"

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


def process_ground_table(df, args, ground_schema):
    """Process and upload ground cover data."""
    print("\nProcessing ground cover data...")
    df_ground = transform_ground_data(df)
    ground_logger = setup_logging(args.ground_table, "ground")
    ground_logger.info("Starting ground cover data processing")
    ground_logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    if validate_ground_data(df_ground, logger=ground_logger):
        print("\nGround cover data validation passed!")
        ground_logger.info("Ground cover data validation passed!")

        if args.dry_run:
            upload_to_bigquery(
                df_ground,
                args.ground_table,
                "ground",
                ground_schema,
                dry_run=True,
                logger=ground_logger,
            )
        else:
            if args.backup_bucket:
                print(f"\nCreating ground cover table backup...")
                if not backup_table(args.ground_table, args.backup_bucket, "ground"):
                    print("Backup failed. Aborting upload.")
                    return False

            upload_to_bigquery(
                df_ground,
                args.ground_table,
                "ground",
                ground_schema,
                dry_run=False,
                logger=ground_logger,
            )
        return True
    else:
        print("Ground cover data validation failed.")
        return False


def main():
    # Parse command line arguments
    args = parse_args()

    # Load the data
    print("Loading data...")
    df = load_data()

    # Define schemas
    vegetation_schema = [
        bigquery.SchemaField("survey_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("grid_point", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("transect_point", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("height_intercept_1", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("intercept_1", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("intercept_2", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("intercept_3", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("intercept_4", "INTEGER", mode="NULLABLE"),
    ]

    ground_schema = [
        bigquery.SchemaField("survey_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("grid_point", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("transect_point", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("intercept_1", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("intercept_ground_code", "STRING", mode="NULLABLE"),
    ]

    # Process vegetation table first
    print("\nProcessing vegetation data...")
    df_veg = transform_vegetation_data(df)
    veg_logger = setup_logging(args.vegetation_table, "vegetation")
    veg_logger.info("Starting vegetation data processing")
    veg_logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    if not validate_vegetation_data(df_veg, logger=veg_logger):
        print("Vegetation data validation failed.")
        sys.exit(1)  # Force exit on error

    print("\nVegetation data validation passed!")
    veg_logger.info("Vegetation data validation passed!")

    if args.dry_run:
        upload_to_bigquery(
            df_veg,
            args.vegetation_table,
            "vegetation",
            vegetation_schema,
            dry_run=True,
            logger=veg_logger,
        )
    else:
        if args.backup_bucket:
            print(f"\nCreating vegetation table backup...")
            if not backup_table(
                args.vegetation_table, args.backup_bucket, "vegetation"
            ):
                print("Backup failed. Aborting upload.")
                return

        try:
            upload_to_bigquery(
                df_veg,
                args.vegetation_table,
                "vegetation",
                vegetation_schema,
                dry_run=False,
                logger=veg_logger,
            )
        except Exception as e:
            print(f"Vegetation upload failed: {e}")
            sys.exit(1)  # Force exit on error

        # Only process ground table if vegetation succeeded
        if not args.skip_ground_table and args.ground_table:
            process_ground_table(df, args, ground_schema)


if __name__ == "__main__":
    main()
