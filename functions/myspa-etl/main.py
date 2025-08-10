from google.cloud import storage
from google.cloud import bigquery
from google.cloud import storage
from elt import process_customer, process_order, process_level
from helper.etl_helper import send_discord_message

def main(event, context):
    bucket_name = event['bucket']
    file_name = event['name']

    try:
        gcs = storage.Client()
        bucket = gcs.bucket(bucket_name)
        bq = bigquery.Client()

        if file_name.startswith('customer/') and file_name.endswith('.xlsx'):
            print(f"Processing file: {file_name}")
            process_customer(file_name, bucket, bq)
        elif file_name.startswith('order/') and file_name.endswith('.xlsx'):
            print(f"Processing file: {file_name}")
            process_order(file_name, bucket, bq)
        elif file_name.startswith('level/') and file_name.endswith('.xlsx'):
            print(f"Processing file: {file_name}")
            process_level(file_name, bucket, bq)
        else:
            return "Skip"
        
        print("Success")
        return "Success"
    
    except Exception as e:
        send_discord_message(f"Error myspa-etl, file {file_name}: {e}")
        # raise e
        print(e)
        return "Fail"





