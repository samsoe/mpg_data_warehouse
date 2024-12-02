import pandas as pd
from pathlib import Path
from google.cloud import bigquery


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

    # Convert date format from MM/DD/YYYY to YYYY-MM-DD
    df_transformed["date"] = pd.to_datetime(df_transformed["date"]).dt.strftime(
        "%Y-%m-%d"
    )

    # Convert grid_point to integer
    df_transformed["grid_point"] = pd.to_numeric(df_transformed["grid_point"])

    # Handle empty species values
    df_transformed["key_plant_species"] = pd.to_numeric(
        df_transformed["key_plant_species"], errors="coerce"
    )

    # Take first 8 characters of UUID for survey_ID
    df_transformed["survey_ID"] = df_transformed["survey_ID"].str[:8]

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


def upload_to_bigquery(df):
    """Upload the transformed data to BigQuery."""
    client = bigquery.Client()
    table_id = "mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species"

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

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"\nSuccessfully uploaded {len(df)} rows to BigQuery")
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")


def main():
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

        # Confirm before upload
        response = input(
            "\nDo you want to proceed with the upload to BigQuery? (y/n): "
        )
        if response.lower() == "y":
            upload_to_bigquery(df_transformed)
        else:
            print("Upload cancelled.")
    else:
        print("Data validation failed. Please check the data before uploading.")


if __name__ == "__main__":
    main()
