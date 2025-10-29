from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os


def load_or_query_metadata():
    """Load metadata from interim CSV if it exists, otherwise query BigQuery"""
    interim_dir = "data/interim"
    interim_file = os.path.join(interim_dir, "gridveg_metadata.csv")

    # Check if interim file exists
    if os.path.exists(interim_file):
        print(f"Loading metadata from {interim_file}...")
        return pd.read_csv(interim_file, parse_dates=["date"])

    # If file doesn't exist, query BigQuery
    print("Querying BigQuery...")
    client = bigquery.Client()
    table_id = (
        "mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata"
    )

    query = f"""
    SELECT *
    FROM `{table_id}`
    """

    df = client.query(query).to_dataframe()

    # Save to interim directory
    os.makedirs(interim_dir, exist_ok=True)
    df.to_csv(interim_file, index=False)
    print(f"Data saved to {interim_file}")

    return df


def explore_metadata():
    # Create visualization directory if it doesn't exist
    viz_dir = "visualizations"
    os.makedirs(viz_dir, exist_ok=True)

    # Get absolute path of visualization directory
    viz_dir_abs = os.path.abspath(viz_dir)
    print(f"\nSaving visualizations to: {viz_dir_abs}")

    # Get data
    df = load_or_query_metadata()

    # Basic data info
    print("\nDataset Info:")
    print(df.info())
    print("\nSample of data:")
    print(df.head())

    # Analyze categorical columns
    categorical_columns = df.select_dtypes(include=["object"]).columns
    for col in categorical_columns:
        print(f"\nUnique values in {col}:")
        value_counts = df[col].value_counts()
        print(value_counts)

        # Skip visualization for surveyor column
        if col == "surveyor":
            continue

        # Create bar plot for categorical variables
        plt.figure(figsize=(10, 6))

        # Special handling for survey_sequence to order by year
        if col == "survey_sequence":
            # Extract year from survey_sequence (handling both formats: "2011-12" and "2019")
            df["seq_year"] = df[col].str.split("-").str[0]
            # Sort by year and sequence number
            value_counts = df.groupby(col).size().reset_index()
            value_counts["year"] = value_counts[col].str.split("-").str[0]
            value_counts = value_counts.sort_values("year")[col].value_counts()

        value_counts.plot(kind="bar")
        plt.title(f"Distribution of {col}")
        plt.xlabel(col)
        plt.ylabel("Count")
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save the figure
        viz_filename = os.path.join(viz_dir, f"metadata_distribution_{col}.png")
        plt.savefig(viz_filename, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved visualization: {os.path.abspath(viz_filename)}")

    # Analyze survey_date if it exists
    if "date" in df.columns:
        print("\nTemporal Analysis:")

        # Basic date statistics
        print("\nDate range:")
        print(f"Earliest date: {df['date'].min()}")
        print(f"Latest date: {df['date'].max()}")

        # Create temporal visualizations
        fig = plt.figure(figsize=(12, 6))

        # Count by year
        df["year"] = df["date"].dt.year
        yearly_counts = df["year"].value_counts().sort_index()

        plt.subplot(1, 2, 1)
        yearly_counts.plot(kind="bar")
        plt.title("Surveys by Year")
        plt.xlabel("Year")
        plt.ylabel("Count")
        plt.xticks(rotation=45)

        # Count by month
        df["month"] = df["date"].dt.month
        monthly_counts = df["month"].value_counts().sort_index()

        plt.subplot(1, 2, 2)
        monthly_counts.plot(kind="bar")
        plt.title("Surveys by Month")
        plt.xlabel("Month")
        plt.ylabel("Count")
        plt.xticks(rotation=45)

        plt.tight_layout()

        # Save the figure
        viz_filename = os.path.join(viz_dir, f"metadata_temporal_analysis.png")
        plt.savefig(viz_filename, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Saved visualization: {os.path.abspath(viz_filename)}")

    # Analyze numeric columns
    numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns
    if len(numeric_columns) > 0:
        print("\nNumeric Column Statistics:")
        print(df[numeric_columns].describe())

        # Create histograms for numeric variables
        for col in numeric_columns:
            plt.figure(figsize=(10, 6))
            sns.histplot(data=df, x=col)
            plt.title(f"Distribution of {col}")
            plt.xlabel(col)
            plt.ylabel("Count")
            plt.tight_layout()

            # Save the figure
            viz_filename = os.path.join(viz_dir, f"metadata_histogram_{col}.png")
            plt.savefig(viz_filename, dpi=300, bbox_inches="tight")
            plt.close()
            print(f"Saved visualization: {os.path.abspath(viz_filename)}")

    return df


if __name__ == "__main__":
    df = explore_metadata()
