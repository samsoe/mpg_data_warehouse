from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def connect_to_bigquery():
    """Create a BigQuery client"""
    return bigquery.Client()


def compare_dates_between_tables(client):
    """Compare dates between additional_species and survey_metadata tables"""
    query = """
    WITH additional_species_dates AS (
        SELECT DISTINCT
            survey_ID,
            date as species_date,
            COUNT(*) as species_record_count,
            STRING_AGG(CAST(date as STRING) ORDER BY date LIMIT 3) as sample_dates
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
        GROUP BY survey_ID, date
    ),
    metadata_dates AS (
        SELECT DISTINCT
            survey_ID,
            date as metadata_date,
            COUNT(*) as metadata_record_count
        FROM 
            `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata`
        GROUP BY survey_ID, date
    )
    SELECT
        a.survey_ID,
        a.species_date,
        m.metadata_date,
        a.species_record_count,
        m.metadata_record_count,
        a.sample_dates,
        CASE 
            WHEN m.survey_ID IS NULL THEN 'Missing in Metadata'
            WHEN a.species_date != m.metadata_date THEN 'Date Mismatch'
            ELSE 'Match'
        END as status,
        EXTRACT(YEAR FROM a.species_date) as species_year,
        EXTRACT(MONTH FROM a.species_date) as species_month,
        EXTRACT(DAY FROM a.species_date) as species_day,
        EXTRACT(YEAR FROM m.metadata_date) as metadata_year,
        EXTRACT(MONTH FROM m.metadata_date) as metadata_month,
        EXTRACT(DAY FROM m.metadata_date) as metadata_day
    FROM additional_species_dates a
    LEFT JOIN metadata_dates m
    ON a.survey_ID = m.survey_ID
    ORDER BY a.species_date
    """

    df = client.query(query).to_dataframe()
    
    # Convert dates to datetime
    df['species_date'] = pd.to_datetime(df['species_date'])
    df['metadata_date'] = pd.to_datetime(df['metadata_date'])
    
    return analyze_discrepancies(df)


def analyze_discrepancies(df):
    """Analyze the discrepancies between the two tables"""
    print("\nDiscrepancy Analysis:")
    print(f"Total number of unique survey_ID/date combinations: {len(df)}")
    
    # Analyze status distribution
    status_counts = df['status'].value_counts()
    print("\nStatus Distribution:")
    for status, count in status_counts.items():
        print(f"{status}: {count} ({count/len(df)*100:.1f}%)")

    # Analyze date components for mismatches
    mismatches = df[df['status'] == 'Date Mismatch']
    if not mismatches.empty:
        print("\nDate Component Analysis for Mismatches:")
        print("\nYear differences:")
        year_diff = mismatches['metadata_year'] - mismatches['species_year']
        print(year_diff.value_counts().sort_index())
        
        print("\nMonth differences:")
        month_diff = mismatches['metadata_month'] - mismatches['species_month']
        print(month_diff.value_counts().sort_index())
        
        print("\nDay differences:")
        day_diff = mismatches['metadata_day'] - mismatches['species_day']
        print(day_diff.value_counts().sort_index())
        
        # Check for potential date format issues
        print("\nPotential date format issues:")
        suspicious = mismatches[
            (mismatches['species_day'] > 12) |  # Day that could be a year
            (mismatches['species_month'] > 12)   # Invalid month
        ]
        if not suspicious.empty:
            print(f"Found {len(suspicious)} records with suspicious date components")
            print("\nSample of suspicious records:")
            print(suspicious[['survey_ID', 'species_date', 'metadata_date', 
                            'species_day', 'species_month', 'species_year']].head())

    return df


def analyze_survey_id_patterns(client):
    """Analyze patterns in survey_IDs between the tables"""
    query = """
    WITH species_surveys AS (
        SELECT DISTINCT survey_ID
        FROM `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_additional_species`
    ),
    metadata_surveys AS (
        SELECT DISTINCT survey_ID
        FROM `mpg-data-warehouse.vegetation_point_intercept_gridVeg.gridVeg_survey_metadata`
    )
    SELECT
        'Only in Additional Species' as location,
        survey_ID
    FROM species_surveys
    WHERE survey_ID NOT IN (SELECT survey_ID FROM metadata_surveys)
    UNION ALL
    SELECT
        'Only in Metadata' as location,
        survey_ID
    FROM metadata_surveys
    WHERE survey_ID NOT IN (SELECT survey_ID FROM species_surveys)
    ORDER BY location, survey_ID
    """

    df = client.query(query).to_dataframe()
    
    print("\nSurvey ID Pattern Analysis:")
    print(df['location'].value_counts())
    
    return df


def plot_discrepancies(df):
    """Create visualizations of the discrepancies"""
    # Create a figure with multiple subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # Plot 1: Status Distribution
    status_counts = df['status'].value_counts()
    status_counts.plot(kind='bar', ax=ax1)
    ax1.set_title('Distribution of Match Status')
    ax1.set_ylabel('Count')
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # Plot 2: Timeline of Discrepancies
    for status in df['status'].unique():
        subset = df[df['status'] == status]
        ax2.scatter(subset['species_date'], 
                   [status] * len(subset), 
                   alpha=0.5, 
                   label=status)
    
    ax2.set_title('Timeline of Discrepancies')
    ax2.set_xlabel('Date')
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    ax2.legend()

    plt.tight_layout()
    plt.savefig('gridveg_discrepancies.png')


def analyze_year_offset_pattern(df):
    """Analyze the apparent 11-year offset pattern in dates"""
    mismatches = df[df['status'] == 'Date Mismatch'].copy()
    
    # Calculate year differences
    mismatches['year_difference'] = (
        mismatches['metadata_date'].dt.year - mismatches['species_date'].dt.year
    )
    
    print("\nYear Difference Analysis:")
    year_diff_counts = mismatches['year_difference'].value_counts().sort_index()
    print(year_diff_counts)
    
    # Check if dates would match if we add 11 years to species_date
    mismatches['adjusted_species_date'] = mismatches['species_date'] + pd.DateOffset(years=11)
    matches_after_adjustment = (
        mismatches['adjusted_species_date'].dt.date == 
        mismatches['metadata_date'].dt.date
    ).sum()
    
    print(f"\nRecords that would match after +11 year adjustment: "
          f"{matches_after_adjustment} out of {len(mismatches)} "
          f"({matches_after_adjustment/len(mismatches)*100:.1f}%)")
    
    return mismatches


def analyze_date_format_pattern(df):
    """Analyze if dates match when correctly interpreting DD-MM-YY format"""
    mismatches = df[df['status'] == 'Date Mismatch'].copy()
    
    # Extract components assuming YYYY-MM-DD was incorrectly interpreted from DD-MM-YY
    mismatches['original_day'] = mismatches['species_date'].dt.year  # Current year is original day
    mismatches['original_month'] = mismatches['species_date'].dt.month
    mismatches['original_year'] = mismatches['species_date'].dt.day  # Current day is original year
    
    # Reconstruct the date in correct format (adding 2000 to year since it's YY format)
    mismatches['reconstructed_date'] = pd.to_datetime(
        {
            'year': mismatches['original_year'] + 2000,
            'month': mismatches['original_month'],
            'day': mismatches['original_day']
        }
    )
    
    # Check how many dates match after reconstruction
    matches_after_reconstruction = (
        mismatches['reconstructed_date'].dt.date == 
        mismatches['metadata_date'].dt.date
    ).sum()
    
    print("\nDate Format Analysis:")
    print(f"Records that match after DD-MM-YY reconstruction: "
          f"{matches_after_reconstruction} out of {len(mismatches)} "
          f"({matches_after_reconstruction/len(mismatches)*100:.1f}%)")
    
    # Show some examples
    print("\nSample of reconstructed dates:")
    sample_df = mismatches[['survey_ID', 'species_date', 'metadata_date', 'reconstructed_date']].head()
    print(sample_df)
    
    return mismatches


def main():
    client = connect_to_bigquery()
    
    # Compare dates between tables
    discrepancies_df = compare_dates_between_tables(client)
    
    # Analyze year offset pattern
    year_pattern_df = analyze_year_offset_pattern(discrepancies_df)
    
    # Analyze survey ID patterns
    survey_patterns_df = analyze_survey_id_patterns(client)
    
    # Create visualizations
    plot_discrepancies(discrepancies_df)
    
    # Save results to CSV for further analysis
    discrepancies_df.to_csv('gridveg_date_discrepancies.csv', index=False)
    survey_patterns_df.to_csv('gridveg_survey_patterns.csv', index=False)
    year_pattern_df.to_csv('gridveg_year_pattern_analysis.csv', index=False)


if __name__ == "__main__":
    main() 