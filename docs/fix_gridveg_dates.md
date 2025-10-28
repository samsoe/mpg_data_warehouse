# GridVeg Date Fix Script

## Overview

`fix_gridveg_dates.py` is a script to correct date transformation issues in the `gridVeg_additional_species` table. It fixes dates that were incorrectly transformed from DD-MM-YY format by having the day (DD) prefixed with '20' and treated as the year.

## How the Fix Works

### Issue Description
The table contains dates that were incorrectly transformed from DD-MM-YY format. For example:
- Original format: `25-05-11` (25th May 2011)
- Incorrect transformation: `2025-05-11` (the day '25' was prefixed with '20')
- Correct date: `2011-05-25`

### Fix Implementation
1. **Identification**: The script identifies affected records by:
   - Querying dates after 2024-12-31 (all incorrect dates start with '20' + day)
   - Joining with `gridVeg_survey_metadata` table which contains correct dates
   - Matching records using `survey_ID`

2. **SQL Logic**:
   ```sql
   -- Preview query to identify affected records
   WITH to_fix AS (
       SELECT 
           a.survey_ID,
           a.date as incorrect_date,
           m.date as correct_date,
           a.year as incorrect_year,
           EXTRACT(YEAR FROM m.date) as correct_year,
           COUNT(*) as affected_rows
       FROM 
           `{project}.{dataset}.{table}` a
       JOIN 
           `{project}.{dataset}.gridVeg_survey_metadata` m
       ON 
           a.survey_ID = m.survey_ID
       WHERE 
           a.date > '2024-12-31'
       GROUP BY 
           a.survey_ID, a.date, m.date, a.year
   )

   -- Update query to fix dates
   UPDATE `{project}.{dataset}.{table}` a
   SET 
       date = m.date,
       year = EXTRACT(YEAR FROM m.date)
   FROM `{project}.{dataset}.gridVeg_survey_metadata` m
   WHERE a.survey_ID = m.survey_ID
   AND a.date > '2024-12-31'
   ```

3. **Correction Process**:
   - Creates backup of the table
   - Updates both the `date` and `year` columns
   - Uses metadata table's dates as the source of truth
   - Only affects records with dates after 2024-12-31

4. **Validation**:
   - Verifies no future dates exist after the fix
   - Ensures year column matches the corrected date
   - Confirms all updates match metadata dates

## Features

- Dry run mode for previewing changes
- Automatic backup to Google Cloud Storage before changes
- Validation checks before and after updates
- Detailed logging of all operations

## Prerequisites

- Python 3.x
- Google Cloud SDK configured
- BigQuery access to the relevant project and dataset
- Storage access to the backup bucket

## Installation

```bash
pip install google-cloud-bigquery google-cloud-storage pandas
```

## Usage

### Preview Changes (Dry Run)

```bash
python src/data/fix_gridveg_dates.py \
  --project-id PROJECT_ID \
  --dataset-id DATASET_ID \
  --table-id gridVeg_additional_species \
  --bucket-name BACKUP_BUCKET \
  --dry-run
```

### Apply Changes

```bash
python src/data/fix_gridveg_dates.py \
  --project-id PROJECT_ID \
  --dataset-id DATASET_ID \
  --table-id gridVeg_additional_species \
  --bucket-name BACKUP_BUCKET
```

## Arguments

- `--project-id`: GCP project ID
- `--dataset-id`: BigQuery dataset ID
- `--table-id`: BigQuery table ID (default: gridVeg_additional_species)
- `--bucket-name`: GCS bucket for backups
- `--dry-run`: Preview changes without applying them

## Backup and Recovery

Backups are automatically created in GCS before any changes:
```
gs://BACKUP_BUCKET/backups/DATASET_ID/TABLE_ID/TIMESTAMP/*.csv
```

## Logging

Logs are written to both stdout and a log file:
```
logs/fix_gridveg_dates_YYYYMMDD_HHMMSS.log
```

The logs include:
- Total records to be updated
- Date ranges (incorrect and correct)
- Sample of changes
- Validation results
- Backup locations

## Validation Checks

The script performs several validations:
1. No future dates after fix
2. Year column matches extracted year from date
3. All updates match metadata dates
4. Backup verification before changes

## Error Handling

The script will abort and log errors if:
- Required permissions are missing
- Backup creation fails
- Validation checks fail
- Any update operations fail

## Support

For issues or questions, contact the data team. 