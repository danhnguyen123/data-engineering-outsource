
import pandas_gbq
import pandas as pd
from config import config
from google.cloud import bigquery
import requests
import re, unicodedata
import pytz

tz = pytz.timezone("Asia/Bangkok")

def get_datetime_local():
    current_time = datetime.now(tz).replace(microsecond=0).replace(tzinfo=None)
    return current_time

def upsert_bigquery(table_name: str, identifier_cols: list, dataframe: pd.DataFrame, bigquery: bigquery.Client):
    staging_table = f'{config.PROJECT_ID}.{config.DATASET_STAGING_ID}.{table_name}'
    destination_table = f'{config.PROJECT_ID}.{config.DATASET_ID}.{table_name}'

    # --- Load to staging table ---
    try:
        bigquery.get_table(staging_table)
    except Exception:
        print(f"Staging table {staging_table} does not exist. It will be created.")

    pandas_gbq.to_gbq(
        dataframe=dataframe,
        destination_table=staging_table,
        project_id=config.PROJECT_ID,
        if_exists="replace"
    )

    # --- If destination table doesn't exist, create it directly ---
    try:
        bigquery.get_table(destination_table)
        print(f"Destination table {destination_table} exists. Proceeding with MERGE.")
    except Exception:
        print(f"Destination table {destination_table} does not exist. Creating with full load.")
        pandas_gbq.to_gbq(
            dataframe=dataframe,
            destination_table=destination_table,
            project_id=config.PROJECT_ID,
            if_exists="replace"
        )
        return "Success"

    # --- Prepare merge SQL ---
    on_clause = " AND ".join([f"target.{col} = source.{col}" for col in identifier_cols])
    update_clause = ", ".join([f"target.{col} = source.{col}" for col in dataframe.columns if col not in identifier_cols])
    insert_columns = ", ".join(dataframe.columns)
    insert_values = ", ".join([f"source.{col}" for col in dataframe.columns])

    merge_sql = f"""
    MERGE `{destination_table}` AS target
    USING `{staging_table}` AS source
    ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """

    print("Executing MERGE query:")
    print(merge_sql)

    query_job = bigquery.query(merge_sql)
    query_job.result()  # Wait for the job to finish

    return "Success"


def send_discord_message(content):
    data = {
        "content": content  # Message content
    }
    response = requests.post(config.DISCORD_WEBHOOK, json=data)


def clean_column(name):
    name = unicodedata.normalize('NFD', name)
    name = name.encode('ascii', 'ignore').decode('utf-8')
    name = name.lower()

    name = name.replace('/', '_')

    name = re.sub(r'[^\w\s]', '', name)  
    name = re.sub(r'\s+', '_', name)     
    name = re.sub(r'[^a-z0-9_]', '', name) 
    return name

