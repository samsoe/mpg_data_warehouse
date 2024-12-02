import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def load_data():
    """Load the additional species data from CSV file."""
    file_path = Path("data/external/2024-10-21_gridVeg_additional_species_SOURCE.csv")
    df = pd.read_csv(file_path)
    return df


def analyze_data(df):
    """Perform basic analysis on the dataset."""
    # Basic statistics
    print("\nDataset Overview:")
    print(f"Total number of records: {len(df)}")
    print(f"Number of unique surveys: {df['Survey Data::__kp_Survey'].nunique()}")
    print(f"Number of unique sites: {df['Survey Data::_kf_Site'].nunique()}")
    print(f"Number of unique species: {df['_kf_Species_serial'].nunique()}")

    # Temporal distribution
    print("\nSurvey Date Range:")
    print(f"First survey: {df['Survey Data::SurveyDate'].min()}")
    print(f"Last survey: {df['Survey Data::SurveyDate'].max()}")

    # Species frequency
    species_counts = df["_kf_Species_serial"].value_counts()
    print("\nTop 10 most common species (by serial number):")
    print(species_counts.head(10))

    return species_counts


def plot_species_distribution(species_counts):
    """Create a bar plot of the most common species."""
    plt.figure(figsize=(12, 6))
    species_counts.head(20).plot(kind="bar")
    plt.title("Distribution of Top 20 Most Common Species")
    plt.xlabel("Species Serial Number")
    plt.ylabel("Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("species_distribution.png")


def main():
    # Load the data
    print("Loading data...")
    df = load_data()

    # Analyze the data
    species_counts = analyze_data(df)

    # Create visualization
    print("\nCreating species distribution plot...")
    plot_species_distribution(species_counts)

    print("\nAnalysis complete. Check species_distribution.png for the visualization.")


if __name__ == "__main__":
    main()
