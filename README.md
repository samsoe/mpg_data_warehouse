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

### Scripts

#### Point Intercepts Update

Updates vegetation point intercept tables in BigQuery.

```bash
# Dry run to validate data
python src/data/point_intercepts_update.py \
  --vegetation-table project.dataset.vegetation_table \
  --ground-table project.dataset.ground_table \
  --dry-run

# Upload with backup
python src/data/point_intercepts_update.py \
  --vegetation-table project.dataset.vegetation_table \
  --ground-table project.dataset.ground_table \
  --backup-bucket bucket-name
```

#### Image Metadata Update

Updates the image metadata table in BigQuery with reference photo information.

```bash
# Dry run to validate data
python src/data/image_metadata_update.py \
  --table-id project.dataset.gridVeg_image_metadata \
  --dry-run

# Upload with backup
python src/data/image_metadata_update.py \
  --table-id project.dataset.gridVeg_image_metadata \
  --backup-bucket bucket-name
```

#### Additional Species Update

Updates additional species tables in BigQuery.

```bash
# Dry run to validate data
python src/data/additional_species_update.py \
  --table-id project.dataset.table \
  --dry-run

# Upload with backup
python src/data/additional_species_update.py \
  --table-id project.dataset.table \
  --backup-bucket bucket-name
```

Both scripts support:
- Dry run mode for validation
- Data type verification
- Logging of upload operations
- Automatic table backup to GCS (optional)

### Logs
Upload logs are stored in the `logs` directory with timestamps and table names.
Table backups are stored in GCS using the pattern: `gs://BUCKET/backups/TABLE/TYPE/TIMESTAMP/`