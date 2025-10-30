## MPG Ranch Data Warehouse

Tools for managing and updating the MPG Ranch data warehouse.

## Data Processing Scripts

Scripts for processing and uploading vegetation survey data to BigQuery.

### Setup

1. Create conda environment:
   ```bash
   conda env create -f environment.yml
   ```

2. Activate environment:
   ```bash
   conda activate gcloud
   ```

3. Configure sensitive data (for notebooks):
   ```bash
   cp config.example.yml config.yml
   # Edit config.yml with your actual GCS URLs and BigQuery table IDs
   ```

### Notebooks

Jupyter notebooks for interactive data updates and exploration.

#### Plant Species Metadata Update

Updates plant species metadata in BigQuery from a CSV file stored in GCS.

**Location:** `notebooks/update_plant_species_metadata.ipynb`

**Documentation:** [Vegetation Species Metadata Documentation](https://docs.google.com/document/d/1aRGYGPsuHmuOxi29bfdzaackjJkfCEY3T2Z3rekQUbY/edit?usp=sharing)

**Configuration:**
- Copy `config.example.yml` to `config.yml` and fill in:
  - `gcs.csv_url`: GCS path to your plant species CSV
  - `bigquery.table_id`: BigQuery table ID (format: `project.dataset.table`)
  - `gcs.backup_bucket`: (Optional) GCS bucket for table backups

**Features:**
- Reads CSV directly from Google Cloud Storage
- YAML-based configuration (sensitive data excluded from git)
- Automatic backup of existing table before updates
- Compare differences between new and existing data
- Data validation and exploration
- Interactive BigQuery updates

### Scripts

#### Survey Metadata Update

Updates the survey metadata table in BigQuery with vegetation survey information.

```bash
# Dry run to validate data
python src/survey_metadata_update.py \
  --table-id project.dataset.gridVeg_survey_metadata \
  --dry-run

# Upload with backup
python src/survey_metadata_update.py \
  --table-id project.dataset.gridVeg_survey_metadata \
  --backup-bucket bucket-name
```

#### Point Intercepts Update

Updates vegetation point intercept tables in BigQuery.

```bash
# Dry run to validate data
python src/point_intercepts_update.py \
  --vegetation-table project.dataset.vegetation_table \
  --ground-table project.dataset.ground_table \
  --dry-run

# Upload with backup
python src/point_intercepts_update.py \
  --vegetation-table project.dataset.vegetation_table \
  --ground-table project.dataset.ground_table \
  --backup-bucket bucket-name
```

#### Image Metadata Update

Updates the image metadata table in BigQuery with reference photo information.

```bash
# Dry run to validate data
python src/image_metadata_update.py \
  --table-id project.dataset.gridVeg_image_metadata \
  --dry-run

# Upload with backup
python src/image_metadata_update.py \
  --table-id project.dataset.gridVeg_image_metadata \
  --backup-bucket bucket-name
```

#### Additional Species Update

Updates additional species tables in BigQuery.

```bash
# Dry run to validate data
python src/additional_species_update.py \
  --table-id project.dataset.table \
  --dry-run

# Upload with backup
python src/additional_species_update.py \
  --table-id project.dataset.table \
  --backup-bucket bucket-name
```

All scripts support:
- Dry run mode for validation
- Data type verification
- Logging of upload operations
- Automatic table backup to GCS (optional)

### Logs
Upload logs are stored in the `logs` directory with timestamps and table names.
Table backups are stored in GCS using the pattern: `gs://BUCKET/backups/TABLE/TYPE/TIMESTAMP/`