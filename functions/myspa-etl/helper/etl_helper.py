
import pandas_gbq
import pandas
from config import config
from google.cloud import bigquery

def upsert_bigquery(table_name: str, identifier_cols: list, dataframe: pandas.DataFrame, bigquery: bigquery.Client):

    staging_table_id = f'{config.PROJECT_ID}.{config.DATASET_STAGING_ID}.{table_name}'

    print(f"Load table to staging table {staging_table_id}")
    pandas_gbq.to_gbq(
        dataframe=dataframe,
        destination_table=staging_table_id,
        if_exists="replace" 
    )
    
    destination_table_id = f'{config.PROJECT_ID}.{config.DATASET_ID}.{table_name}'

    try:
        bigquery.get_table(destination_table_id) 
        print(f"Table {destination_table_id} exists.")
        table_exists = True
    except Exception:
        print(f" Table {destination_table_id} does not exist.")
        table_exists = False
        print(f"Load table {destination_table_id}")
        pandas_gbq.to_gbq(
            dataframe=dataframe,
            destination_table=destination_table_id,
            if_exists="replace" 
        )
        return "Success"

    on_clause = " and ".join([f"target.{col}=source.{col}" for col in identifier_cols])

    table_cols = list(dataframe.columns)

    update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in table_cols if col not in identifier_cols])
    insert_values_clause = ", ".join([f"source.{col}" for col in table_cols])

    merge_query = f"""
    MERGE `{destination_table_id}` AS target
    USING `{staging_table_id}` AS source
    ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN 
        INSERT ({", ".join(table_cols)}) 
        VALUES ({insert_values_clause}) 
    """ 
    print(merge_query)

    query_job = bigquery.query(merge_query) 
