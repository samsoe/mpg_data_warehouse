"""
Configuration Example for Plant Species Metadata Update

Copy this file to config.py and fill in your actual values.
Make sure config.py is in .gitignore to keep sensitive information secure.

Alternatively, use a .env file with these variables:
- PLANT_SPECIES_CSV_URL
- PLANT_SPECIES_TABLE_ID
"""

# GCS URL for the plant species CSV file
# Format: gs://bucket-name/path/to/file.csv
PLANT_SPECIES_CSV_URL = "gs://your-bucket/path/to/plant_species.csv"

# BigQuery table ID for plant species metadata
# Format: project.dataset.table
PLANT_SPECIES_TABLE_ID = "your-project.your_dataset.plant_species_table"

