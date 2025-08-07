import pandas as pd
import re, unicodedata
from io import BytesIO
from helper.etl_helper import upsert_bigquery

def etl_customer(file_name, bucket, bq):

    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(data))

    print(f"Old column:\{df.columns}")

    df.columns = [clean_column(col) for col in df.columns]

    print(f"New column:\{df.columns}")

    #### Load

    upsert_bigquery(table_name='customer', 
                    identifier_cols=['ma_khach_hang'],
                    dataframe=df,
                    bigquery=bq
                    )

def clean_column(name):
    name = unicodedata.normalize('NFD', name)
    name = name.encode('ascii', 'ignore').decode('utf-8')
    name = name.lower()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^a-z0-9_]', '', name)

    return name
