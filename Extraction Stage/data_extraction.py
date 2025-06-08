import os
import pandas as pd
import psycopg2
from google.cloud import bigquery
from google.cloud import bigquery
import time

# PostgreSQL connection parameters
pg_host = "34.141.73.126"
pg_database = 
pg_user = 
pg_password = "
pg_port = "

# Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/me/Desktop/DE10/agent_godson_techy_key.json'

# BigQuery parameters
project_id = 'mh-project-testing'
dataset_id = 'hivee2'
table_name = 'stg_student_engagement'

# SQL query to run against PostgreSQL
query = """
SELECT
    a.id,
    a."firstName",
    a."lastName",
    b."specialization",
    a."country",
    ROUND(
        b."cloudDataIngestionScore" + 
        b."cloudDataPipelinePython" + 
        b."cloudDataPipelineOrchestration" + 
        b."dataTransformationResultValidationSQL" + 
        b."cloudDataStorageRetrievalSecurity"
    ) AS Total_DE_Score,
    ROUND(
        b."mlIntroAndEDA" + 
        b."mlmCreationAndTraining" + 
        b."mlmServingAndPredictions" + 
        b."llmEvaluationAndTuning" + 
        b."llmAndGenerativeAI"
    ) AS total_ml_score,
    b.level,
    b."attemptsCodingTaskSQL",
    b."attemptsCodingTaskPython",
    a."createdOn",
    a."lastRequest",
    a."updatedOn",
    a."email" AS email_address
FROM 
    "User" AS a
JOIN 
    "UserSpecialization" AS b
ON 
    a."id" = b."userId"
WHERE 
    a.id NOT IN (9, 23, 58, 10, 6, 2,3);
"""

def fetch_data_from_postgres(query):
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=pg_host,
        database=pg_database,
        user=pg_user,
        password=pg_password,
        port=pg_port
    )

    # Fetch data
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_data_to_bigquery(df):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Set the destination table
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Define table schema if you want to ensure a specific structure
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Load the data to BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Loaded {len(df)} rows into {table_id}")

def execute_stored_procedure(client, dataset_id, procedure_name):
    query = f"CALL `{project_id}.{dataset_id}.{procedure_name}`()"
    query_job = client.query(query)
    query_job.result()  # Wait for the query to finish
    print(f"Stored Procedure {procedure_name} executed successfully.")

if __name__ == "__main__":
    # Fetch data from PostgreSQL
    data = fetch_data_from_postgres(query)

    # Load data to BigQuery
    load_data_to_bigquery(data)

    # Initialize BigQuery client
    client = bigquery.Client()

    # Delay for a short time to ensure data is loaded completely
    time.sleep(10)  # Wait for 10 seconds

    # Execute the first stored procedure
    execute_stored_procedure(client, dataset_id, "sp_cdcv2")

    # Delay for a short time before executing the next procedure
    time.sleep(5)  # Wait for 5 seconds

    # Execute the second stored procedure
    execute_stored_procedure(client, dataset_id, "sp_create_student_engagement")
