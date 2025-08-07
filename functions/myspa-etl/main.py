from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from google.cloud import storage
from io import BytesIO
from modules.customer import etl_customer
from helper.etl_helper import send_discord_message

def main(event, context):
    bucket_name = event['bucket']
    file_name = event['name']

    try:

        # Tải file từ GCS
        gcs = storage.Client()
        bucket = gcs.bucket(bucket_name)
        bq = bigquery.Client()

        # Customer
        if file_name.startswith('customer/') and file_name.endswith('.xlsx'):
            print(f"Processing file: {file_name}")
            etl_customer(file_name, bucket, bq)
        else:
            return "Skip"
        
        return "Success"
    
    except Exception as e:
        send_discord_message(f"Error myspa-etl, file {file_name}: {e}")
        # raise e
        return "Fail"





