from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os


def load_or_query_data():
    """Load data from interim CSV if it exists, otherwise query BigQuery"""
    interim_dir = "data/interim"
    interim_file = os.path.join(interim_dir, "gridveg_data.csv")

    # Check if interim file exists
    if os.path.exists(interim_file):
        print(f"Loading data from {interim_file}...")
        return pd.read_csv(interim_file, parse_dates=["date"])

    # If file doesn't exist, query BigQuery
    print("Querying BigQuery...")
    client = bigquery.Client()
    table_id = "mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species_copy"

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


def explore_gridveg_table():
    # Create visualization directory if it doesn't exist
    viz_dir = "visualizations"
    os.makedirs(viz_dir, exist_ok=True)

    # Get data
    df = load_or_query_data()

    # Basic data info
    print("\nDataset Info:")
    print(df.info())

    # Analyze the 'date' column
    date_col = "date"
    if date_col in df.columns:
        print("\nTemporal Analysis:")
        print(f"\nAnalyzing {date_col}:")

        # Convert dbdate to datetime
        df[date_col] = pd.to_datetime(df[date_col])

        # Basic date statistics
        print("\nDate range:")
        print(f"Earliest date: {df[date_col].min()}")
        print(f"Latest date: {df[date_col].max()}")

        # Create temporal visualizations
        fig = plt.figure(figsize=(12, 6))

        # Count by year
        df[f"{date_col}_year"] = df[date_col].dt.year
        yearly_counts = df[f"{date_col}_year"].value_counts().sort_index()

        plt.subplot(1, 2, 1)
        yearly_counts.plot(kind="bar")
        plt.title(f"Observations by Year\n({date_col})")
        plt.xlabel("Year")
        plt.ylabel("Count")
        plt.xticks(rotation=45)

        # Count by month (across all years)
        df[f"{date_col}_month"] = df[date_col].dt.month
        monthly_counts = df[f"{date_col}_month"].value_counts().sort_index()

        plt.subplot(1, 2, 2)
        monthly_counts.plot(kind="bar")
        plt.title(f"Observations by Month\n({date_col})")
        plt.xlabel("Month")
        plt.ylabel("Count")
        plt.xticks(rotation=45)

        plt.tight_layout()

        # Save the figure
        viz_filename = os.path.join(viz_dir, f"temporal_analysis_{date_col}.png")
        plt.savefig(viz_filename, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"Visualization saved to: {viz_filename}")

        # Create additional monthly distribution plot using seaborn
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=df, x=f"{date_col}_month", y=f"{date_col}_year")
        plt.title(f"Year Distribution by Month\n({date_col})")
        plt.xlabel("Month")
        plt.ylabel("Year")

        # Save the additional plot
        viz_filename = os.path.join(viz_dir, f"monthly_distribution_{date_col}.png")
        plt.savefig(viz_filename, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"Monthly distribution visualization saved to: {viz_filename}")
    else:
        print(f"Warning: '{date_col}' column not found in the dataset")

    return df


if __name__ == "__main__":
    df = explore_gridveg_table()
