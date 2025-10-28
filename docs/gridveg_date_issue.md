# GridVeg Date Issue Investigation

## Issue Description

The `gridVeg_additional_species` table in BigQuery contains incorrect dates due to a date format transformation error. Initial investigation shows:

- Total affected records: 2,340
- Total affected surveys: 242
- Affected date range: 2025-05-11 to 2031-08-16
- Issue: Date format DD-MM-YY was incorrectly transformed by prefixing the day with "20"

## Analysis Findings

### Date Pattern
- Identified cause: The day (DD) from DD-MM-YY format was incorrectly treated as year by prefixing it with "20"
- Examples from the data:
  - DD-MM-YY: 25-05-11 -> Incorrect: 2025-05-11 -> Actual: 2011-05-25
  - DD-MM-YY: 31-08-16 -> Incorrect: 2031-08-16 -> Actual: 2016-08-31
  - DD-MM-YY: 16-07-12 -> Incorrect: 2016-07-12 -> Actual: 2012-07-16
  - DD-MM-YY: 11-07-13 -> Incorrect: 2011-07-13 -> Actual: 2013-07-11
  - DD-MM-YY: 16-07-14 -> Incorrect: 2016-07-14 -> Actual: 2014-07-16

- Pattern characteristics:
  - Original dates were in DD-MM-YY format
  - The transformation incorrectly prefixed DD with "20" making it the year
  - This explains why all incorrect years start with "20"
  - The original day and year were swapped in the process
  - Survey_IDs match between tables, confirming these are the same events

### Metadata Coverage Analysis
- All records with future dates (after 2024) have matching metadata records
- This means we can reliably use the metadata table to fix all incorrect dates

### Validation Points
- All 242 affected surveys have corresponding metadata records
- All 2,340 records with future dates can be validated against metadata
- Survey_IDs provide reliable matching between tables
- The metadata table's dates align with expected survey timeframes

### Affected Tables
The issue appears to be isolated to the `gridVeg_additional_species` table. Related tables checked:
- gridVeg_point_intercept_ground
- gridVeg_shrub_tree
- gridVeg_survey_metadata (contains correct dates)
- gridVeg_image_metadata
- gridVeg_ground_cover_metadata
- gridVeg_point_intercept_vegetation

### Investigation Tools

A Python script (`src/data/investigate_gridveg_dates.py`) was created to analyze:
- Basic date distribution
- Cross-reference with survey metadata
- Pattern analysis of date differences
- Preview of potential fixes
- Metadata coverage analysis

## Next Steps

1. Verify fix approach:
   - Use survey_metadata dates as source of truth
   - Cross-validate all corrections
   - Confirm fix using survey_ID matching

2. Create backup of affected table

3. Implement fix with validation:
   - Create fix script with dry-run option
   - Add validation checks
   - Document rollback procedure
   - Verify all updated records match metadata dates

## Running the Investigation

```bash
# Activate environment
conda activate gcloud

# Run analysis script
python src/data/investigate_gridveg_dates.py
```

The script will generate:
- Date distribution visualization
- Analysis of date patterns
- Preview of proposed fixes
- Metadata coverage statistics 