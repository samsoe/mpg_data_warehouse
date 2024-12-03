## MPG Ranch Data Warehouse

Tools for managing and updating the MPG Ranch data warehouse.

## Additional Species Data

This project includes a script for uploading additional species data to BigQuery.

### BigQuery Upload Script
`additional_species-update.py` - Uploads the additional species data to BigQuery

#### Usage
```bash
python additional_species-update.py --table PROJECT.DATASET.TABLE
```

The script supports:
- Dry run mode for validation
- Data type **verification**
- Logging of upload operations

### Setup

1. Create conda environment:
   ```bash
   conda env create -f environment.yml
   ```

2. Activate environment:
   ```bash
   conda activate species-analysis
   ```

### Logs
Upload logs are stored in the `logs` directory with timestamps and table names.