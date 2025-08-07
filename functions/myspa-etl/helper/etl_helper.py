
import pandas_gbq
import pandas as pd
from config import config
from google.cloud import bigquery

def upsert_bigquery(table_name: str, identifier_cols: list, dataframe: pd.DataFrame, bigquery_client: bigquery.Client):
    staging_table = f'{config.PROJECT_ID}.{config.DATASET_STAGING_ID}.{table_name}'
    destination_table = f'{config.PROJECT_ID}.{config.DATASET_ID}.{table_name}'

    def upload_to_bq(df, table_id):
        print(f"Uploading DataFrame to {table_id}")
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=table_id,
            project_id=config.PROJECT_ID,
            if_exists="replace"
        )

    # --- Load to staging table ---
    try:
        bigquery_client.get_table(staging_table)
        print(f"Staging table {staging_table} exists.")
    except Exception:
        print(f"Staging table {staging_table} does not exist. It will be created.")
    upload_to_bq(dataframe, staging_table)

    # --- If destination table doesn't exist, create it directly ---
    try:
        bigquery_client.get_table(destination_table)
        print(f"Destination table {destination_table} exists. Proceeding with MERGE.")
        table_exists = True
    except Exception:
        print(f"Destination table {destination_table} does not exist. Creating with full load.")
        upload_to_bq(dataframe, destination_table)
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

    query_job = bigquery_client.query(merge_sql)
    query_job.result()  # Wait for the job to finish

    return "Success"
