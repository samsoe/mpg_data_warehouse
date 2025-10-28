"""Fix date transformation issues in gridVeg_additional_species table.

The script first downloads data from BigQuery tables to CSV files, then processes them locally.
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import argparse
import logging
import sys
import os

# Create logs and data directories if they don't exist
os.makedirs("logs", exist_ok=True)
os.makedirs("data/external", exist_ok=True)

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


def download_table_to_csv(client, project_id, dataset_id, table_id):
    """Download a BigQuery table to a CSV file

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID

    Returns:
        str: Path to the saved CSV file
    """
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    """

    logging.info(f"Downloading {table_id}...")
    df = client.query(query).to_dataframe()

    output_path = f"data/external/{table_id}.csv"
    df.to_csv(output_path, index=False)
    logging.info(f"Saved to {output_path}")

    return output_path


def load_or_download_data(client, project_id, dataset_id, table_id):
    """Load data from CSV if it exists, otherwise download from BigQuery

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID

    Returns:
        pd.DataFrame: The loaded data
    """
    csv_path = f"data/external/{table_id}.csv"

    if os.path.exists(csv_path):
        logging.info(f"Loading existing CSV file: {csv_path}")
        return pd.read_csv(csv_path)

    logging.info(f"CSV not found. Downloading {table_id} from BigQuery...")
    return download_table_to_csv(client, project_id, dataset_id, table_id)


def fix_dates(metadata_df, species_df):
    """Replace dates in species dataframe with correct dates from metadata

    Args:
        metadata_df: DataFrame with survey metadata (contains correct dates)
        species_df: DataFrame with species data (contains incorrect dates)

    Returns:
        pd.DataFrame: Species data with corrected dates
    """
    # Create a copy to avoid modifying original data
    fixed_df = species_df.copy()

    # Convert date columns to datetime if they aren't already
    metadata_df["date"] = pd.to_datetime(metadata_df["date"])
    fixed_df["date"] = pd.to_datetime(fixed_df["date"])

    # Before the merge, log some statistics
    logging.info(f"\nBefore date correction:")
    logging.info(
        f"Date range in species data: {fixed_df['date'].min()} to {fixed_df['date'].max()}"
    )
    logging.info(f"Number of records: {len(fixed_df)}")

    # Create a mapping of survey_ID to correct date
    date_mapping = metadata_df[["survey_ID", "date"]].set_index("survey_ID")

    # Replace the dates using the mapping
    fixed_df["date"] = fixed_df["survey_ID"].map(date_mapping["date"])

    # Update the year column to integer based on the date
    fixed_df["year"] = fixed_df["date"].dt.year.astype("int32")

    # After the merge, log the results
    logging.info(f"\nAfter date correction:")
    logging.info(
        f"Date range in species data: {fixed_df['date'].min()} to {fixed_df['date'].max()}"
    )
    logging.info(f"Number of records: {len(fixed_df)}")
    logging.info(f"Year dtype: {fixed_df['year'].dtype}")

    # Check for any missing dates after the merge
    missing_dates = fixed_df[fixed_df["date"].isna()]
    if len(missing_dates) > 0:
        logging.warning(f"Found {len(missing_dates)} records with missing dates!")
        logging.warning("Sample of survey_IDs with missing dates:")
        logging.warning(missing_dates["survey_ID"].head())

    return fixed_df


def main():
    parser = argparse.ArgumentParser(
        description="Download gridVeg tables from BigQuery"
    )
    parser.add_argument("--project-id", required=True, help="BigQuery project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")

    args = parser.parse_args()

    # Create interim directory if it doesn't exist
    os.makedirs("data/interim", exist_ok=True)

    client = connect_to_bigquery()

    try:
        # Load or download both tables
        tables = ["gridVeg_survey_metadata", "gridVeg_additional_species"]
        dataframes = {}

        for table_id in tables:
            df = load_or_download_data(
                client, args.project_id, args.dataset_id, table_id
            )
            dataframes[table_id] = df
            logging.info(f"Loaded {table_id} with shape: {df.shape}")

        metadata_df = dataframes["gridVeg_survey_metadata"]
        species_df = dataframes["gridVeg_additional_species"]

        # Fix the dates
        fixed_species_df = fix_dates(metadata_df, species_df)

        # Save to interim folder
        output_path = "data/interim/gridVeg_additional_species_fixed.csv"
        fixed_species_df.to_csv(output_path, index=False)
        logging.info(f"\nSaved fixed data to: {output_path}")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
