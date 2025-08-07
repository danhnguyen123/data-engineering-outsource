import pandas as pd
import re
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
    name = name.lower()  # lowercase
    name = re.sub(r'[^\w\s]', '', name)  # remove special characters (keep word chars and space)
    name = re.sub(r'\s+', '_', name)  # replace whitespace with underscore
    return name

