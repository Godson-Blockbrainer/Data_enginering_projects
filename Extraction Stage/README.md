# Extraction Stage
The link to an article where i explained extensivly reegarding this project is on my meduim channel:
https://medium.com/@chiegbuugochukwu/extracting-data-for-elt-from-postgresql-to-bigquery-b955e7bafba6

## Introduction
This folder contains the initial stage of an ELT (Extract, Load, Transform) pipeline, focusing on extracting student engagement data from a PostgreSQL database and loading it into Google BigQuery.

## Project Overview
The extraction process retrieves data using a Python script, preparing it for subsequent transformation stages like Change Data Capture (CDC). This stage ensures raw data is available for analysis and synchronization.

## Key Files
- **`data_extraction.py`**: The Python script that connects to PostgreSQL, fetches data with a custom SQL query, and loads it into the `stg_student_engagement` table in BigQuery.
- **Dependencies**: Requires `pandas`, `psycopg2`, and `google-cloud-bigquery` libraries.

## How It Works
1. **Connection**: Establishes a secure connection to PostgreSQL and BigQuery using credentials.
2. **Data Fetch**: Executes a SQL query to extract student details, scores, and engagement metrics.
3. **Data Load**: Transfers the data into a BigQuery staging table, overwriting existing data.

## Usage
1. Install required libraries: `pip install pandas psycopg2-binary google-cloud-bigquery`.
2. Update the script with your PostgreSQL credentials and Google Cloud credentials path.
3. Run the script: `python data_extraction.py`.

## Challenges
- **Large Datasets**: Current script loads all data at once; consider incremental extraction for scalability.
- **Data Consistency**: Query design filters specific IDs, but no validation ensures source match.

## Next Steps
Future articles will cover transformation (e.g., CDC) and visualization stages of this ELT pipeline.

## References
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [Google BigQuery Python Client](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)
