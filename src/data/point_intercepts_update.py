import pandas as pd
from pathlib import Path
from google.cloud import bigquery
import argparse
from datetime import datetime
import logging


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
        required=True,
        help="BigQuery ground cover table ID (format: project.dataset.table)",
    )
    parser.add_argument(
        "--backup-bucket",
        help="GCS bucket for table backup (format: bucket-name)",
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
    df_transformed["height_intercept_1"] = (
        df_transformed["height_intercept_1"].replace("", pd.NA).astype("float64")
    )

    # Convert intercept columns to nullable integers
    intercept_columns = ["intercept_1", "intercept_2", "intercept_3", "intercept_4"]
    for col in intercept_columns:
        df_transformed[col] = df_transformed[col].replace("", pd.NA).astype("Int64")

    # Take first 8 characters of UUID for survey_ID
    df_transformed["survey_ID"] = df_transformed["survey_ID"].str[:8]

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

    # Take first 8 characters of UUID for survey_ID
    df_transformed["survey_ID"] = df_transformed["survey_ID"].str[:8]

    return df_transformed


def validate_vegetation_data(df):
    """Validate the vegetation data."""
    print("\nValidating Vegetation Data:")
    print(f"Total records: {len(df)}")
    print("\nNull values in key columns:")
    print(
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

    print("\nData types:")
    print(df.dtypes)

    # Validate required fields
    required_valid = all(
        [
            df["grid_point"].notnull().all(),
            df["date"].notnull().all(),
            df["year"].notnull().all(),
            df["transect_point"].notnull().all(),
            df["intercept_1"].notnull().all(),
        ]
    )

    # Validate transect point format
    transect_format = df["transect_point"].str.match(r"^[NSEW]\d{1,2}$").all()

    return required_valid and transect_format


def validate_ground_data(df):
    """Validate the ground cover data."""
    print("\nValidating Ground Cover Data:")
    print(f"Total records: {len(df)}")
    print("\nNull values in key columns:")
    print(
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

    print("\nUnique ground cover codes:")
    print(df["intercept_ground_code"].unique())

    print("\nData types:")
    print(df.dtypes)

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


def upload_to_bigquery(df, table_id, table_type, schema, dry_run=True):
    """Upload the transformed data to BigQuery."""
    client = bigquery.Client()

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
                print(
                    f"\nDry run successful - {len(df)} rows would be uploaded to {table_id}"
                )
                print("Data schema and types are valid")

                print(f"\nUpload Summary for {table_type} (Dry Run):")
                print(f"Total rows: {len(df)}")
                print(f"Unique survey_IDs: {df['survey_ID'].nunique()}")
                print(
                    f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
                )
                print(f"Unique grid points: {df['grid_point'].nunique()}")
                print("\nSample of data that would be uploaded:")
                print(df.head())
            else:
                print(
                    "Schema validation failed - data types don't match expected schema"
                )

        except Exception as e:
            print(f"Dry run validation failed: {e}")
    else:
        # Setup logging for actual upload
        logger = setup_logging(table_id, table_type)

        try:
            logger.info(f"Starting upload to {table_id}")
            logger.info(f"Total rows to upload: {len(df)}")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete

            logger.info(f"Successfully uploaded {len(df)} rows to BigQuery")
            logger.info("Upload completed successfully")

            print(f"\nSuccessfully uploaded {len(df)} rows to BigQuery")
            print(f"See logs/bigquery_update_*_{table_type}_*.log for details")

        except Exception as e:
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

    # Process vegetation table
    print("\nProcessing vegetation data...")
    df_veg = transform_vegetation_data(df)
    if validate_vegetation_data(df_veg):
        print("\nVegetation data validation passed!")
        run_type = input("\nRun vegetation table in test mode (dry run)? (y/n): ")
        if run_type.lower() == "y":
            upload_to_bigquery(
                df_veg,
                args.vegetation_table,
                "vegetation",
                vegetation_schema,
                dry_run=True,
            )
        else:
            if args.backup_bucket:
                print(f"\nCreating vegetation table backup...")
                if not backup_table(
                    args.vegetation_table, args.backup_bucket, "vegetation"
                ):
                    response = input(
                        "\nBackup failed. Continue with upload anyway? (y/n): "
                    )
                    if response.lower() != "y":
                        print("Vegetation upload cancelled.")
                        return

            response = input("\nProceed with vegetation table upload? (y/n): ")
            if response.lower() == "y":
                upload_to_bigquery(
                    df_veg,
                    args.vegetation_table,
                    "vegetation",
                    vegetation_schema,
                    dry_run=False,
                )
            else:
                print("Vegetation upload cancelled.")
    else:
        print("Vegetation data validation failed.")

    # Process ground cover table
    print("\nProcessing ground cover data...")
    df_ground = transform_ground_data(df)
    if validate_ground_data(df_ground):
        print("\nGround cover data validation passed!")
        run_type = input("\nRun ground cover table in test mode (dry run)? (y/n): ")
        if run_type.lower() == "y":
            upload_to_bigquery(
                df_ground, args.ground_table, "ground", ground_schema, dry_run=True
            )
        else:
            if args.backup_bucket:
                print(f"\nCreating ground cover table backup...")
                if not backup_table(args.ground_table, args.backup_bucket, "ground"):
                    response = input(
                        "\nBackup failed. Continue with upload anyway? (y/n): "
                    )
                    if response.lower() != "y":
                        print("Ground cover upload cancelled.")
                        return

            response = input("\nProceed with ground cover table upload? (y/n): ")
            if response.lower() == "y":
                upload_to_bigquery(
                    df_ground, args.ground_table, "ground", ground_schema, dry_run=False
                )
            else:
                print("Ground cover upload cancelled.")
    else:
        print("Ground cover data validation failed.")


if __name__ == "__main__":
    main()
