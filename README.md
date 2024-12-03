## MPG Ranch Data Warehouse

Tools for managing and updating the MPG Ranch data warehouse.

## Additional Species Data

This project includes a script for uploading additional species data to BigQuery.

### BigQuery Upload Script
`src/data/additional_species_update.py` - Uploads the additional species data to BigQuery

#### Usage
Basic usage:
```bash
python src/data/additional_species_update.py --table PROJECT.DATASET.TABLE
```

With backup to Google Cloud Storage:
```bash
python src/data/additional_species_update.py \
  --table PROJECT.DATASET.TABLE \
  --backup-bucket BUCKET_NAME
```

Dry run mode (validate without uploading):
```bash
python src/data/additional_species_update.py \
  --table PROJECT.DATASET.TABLE \
  --dry-run
```

## Point Intercepts Data

This project includes a script for uploading vegetation point intercepts data to BigQuery. The data is split into two tables:
- Vegetation data (species and height measurements)
- Ground cover data

### Point Intercepts Upload Script
`src/data/point_intercepts_update.py` - Uploads point intercepts data to both vegetation and ground cover tables. The script:
- Handles nullable integer fields
- Validates data format and required fields
- Supports both vegetation and ground cover tables
- Provides detailed upload logging

### Schema Requirements
#### Vegetation Table
- survey_ID (STRING)
- grid_point (INTEGER)
- date (DATE)
- year (INTEGER)
- transect_point (STRING)
- height_intercept_1 (NUMERIC)
- intercept_1 (INTEGER)
- intercept_2 (INTEGER)
- intercept_3 (INTEGER)
- intercept_4 (INTEGER)

Note: Height measurements may not be present in all surveys (e.g., 2024 data).

#### Ground Cover Table
- survey_ID (STRING)
- grid_point (INTEGER)
- date (DATE)
- year (INTEGER)
- transect_point (STRING)
- intercept_1 (INTEGER)
- intercept_ground_code (STRING)

All fields are nullable. Integer fields use NULLABLE INTEGER type in BigQuery.

#### Usage
Basic usage:
```bash
python src/data/point_intercepts_update.py \
  --vegetation-table PROJECT.DATASET.gridVeg_point_intercept_vegetation \
  --ground-table PROJECT.DATASET.gridVeg_point_intercept_ground
```

With backup to Google Cloud Storage:
```bash
python src/data/point_intercepts_update.py \
  --vegetation-table PROJECT.DATASET.gridVeg_point_intercept_vegetation \
  --ground-table PROJECT.DATASET.gridVeg_point_intercept_ground \
  --backup-bucket BUCKET_NAME
```

Dry run mode (validate without uploading):
```bash
python src/data/point_intercepts_update.py \
  --vegetation-table PROJECT.DATASET.gridVeg_point_intercept_vegetation \
  --ground-table PROJECT.DATASET.gridVeg_point_intercept_ground \
  --dry-run
```

Both scripts support:
- Dry run mode for validation
- Data type verification
- Logging of upload operations
- Automatic table backup to GCS (optional)

### Setup

1. Create conda environment:
   ```bash
   conda env create -f environment.yml
   ```

2. Activate environment:
   ```bash
   conda activate gcloud
   ```

### Logs
Upload logs are stored in the `logs` directory with timestamps and table names.
Table backups are stored in GCS using the pattern: `gs://BUCKET/backups/TABLE/TYPE/TIMESTAMP/`