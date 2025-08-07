import json
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import pandas_gbq
from io import BytesIO
from config import config
from logging import Logger

class GCSHelper:

    def __init__(self, logger: Logger, client=None, credentials_file=None, bucket_name=None):

        self.client = client
        self.logger = logger
        self.bucket_name = bucket_name
        if not self.client:
            self.credentials = service_account.Credentials.from_service_account_file(credentials_file or config.SERVICE_ACCOUNT)
            self.client = storage.Client(credentials=self.credentials)
            self.bucket = self.client.get_bucket(self.bucket_name)

    def upload_json(self, json_string, file_name):

        # Get the specified bucket
        

        # Create a blob with the desired file name
        blob = self.bucket.blob(file_name)

        # Upload the JSON string to the blob
        try:
            blob.upload_from_string(json_string, content_type='application/json; charset=utf-8')
            self.logger.debug(f"JSON file uploaded to GCS: gs://{self.bucket_name}/{file_name}")
        except:
            self.logger.error(f"Error when uploading to gs://{self.bucket_name}/{file_name}")

    def download_json(self, blob):
        try:
            json_string = blob.download_as_text()
            self.logger.debug(f"JSON file downloaded from GCS: gs://{self.bucket_name}/{blob.name}")
            return json_string 
        except:
            self.logger.error(f"Error when downloading from GCS://{self.bucket_name}/{blob.name}")    

    def download_file(self, blob_name):
        blob = self.bucket.blob(blob_name) 
        file_bytes = blob.download_as_bytes()
        file = BytesIO(file_bytes)
        return file

class BQHelper:

    def __init__(self, logger: Logger, client=None, credentials_file=None):

        self.client = client
        self.logger = logger
        if not self.client:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file or config.SERVICE_ACCOUNT, 
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)


    def get_table(self, table_id='project-id.dataset-id.table-id'):

        # TODO(developer): Set table_id to the ID of the model to fetch.
        # table_id = 'your-project.your_dataset.your_table'

        table = self.client.get_table(table_id)  # Make an API request.

        # View table properties
        self.logger.debug(
            "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
        )
        self.logger.debug("Table schema: {}".format(table.schema))
        self.logger.debug("Table description: {}".format(table.description))
        self.logger.debug("Table has {} rows".format(table.num_rows))

        # SELECT * EXCEPT(is_typed)
        # FROM mydataset.INFORMATION_SCHEMA.TABLES

    def execute(self, query):
        query_job = self.client.query(query)
        results = query_job.result()
        return results

    def select(self, query):
        try:
            data_frame = pandas_gbq.read_gbq(query,credentials=self.credentials,progress_bar_type=None)
        except Exception as e:
            self.logger.debug(e)
            return False
        return data_frame
    
    def bq_append(self, update_data, table_name, dataset_id, if_exists='append', load_method="load_csv", project_id=config.PROJECT_ID):
        if update_data is None or update_data.shape[0] == 0:
            return False, "Empty DataFrame"
            # update_data=load_data.copy()

        table_id = f'{project_id}.{dataset_id}.{table_name}'

        try:
            if load_method == "load_csv":
                for c in update_data.columns:
                    type = str(update_data[c].dtypes)
                    if type  == 'object':
                        update_data[c] = update_data[c].astype("string")
                    elif type  == 'datetime64[ns, UTC]':
                        update_data[c] = update_data[c].dt.strftime(config.DWH_TIME_FORMAT)
                        update_data[c] = pd.to_datetime(update_data[c],errors='coerce',format="%Y-%m-%d %H:%M:%S")

            pandas_gbq.to_gbq(update_data, destination_table=table_id,chunksize=50000,if_exists=if_exists,credentials=self.credentials, api_method=load_method)
        except Exception as e:
            self.logger.error(e)
            raise e

    # def bq_upsert(self, update_data, table_name, db_schema, pk_array  =[], if_exists='append', project_id=config.PROJECT_ID, credentials_file = cf.GOOGLE_SA_UPLOAD):
    #     if update_data is None or update_data.shape[0] == 0:
    #         return False, "Empty DataFrame"
    #         # update_data=load_data.copy()

    #     credentials = service_account.Credentials.from_service_account_file(credentials_file)
    #     table_id = '{dataset}.{table}'.format(dataset=db_schema,table=table_name)

    #     key = update_data[pk_array].copy()
    #     for c in key.columns:
    #         key[c] = key[c].apply(str)

    #     update_data['insertID'] = key.agg('-'.join, axis=1)
    #     update_data['loadTime'] = th.get_now()

    #     # Drop column na
    #     update_data=update_data.dropna(axis=1,how='all')

    #     for c in update_data.columns:
    #         type = str(update_data[c].dtypes)
    #         if type  == 'object':
    #             update_data[c] = update_data[c].astype("string")
    #         elif type  == 'datetime64[ns, UTC]':
    #             update_data[c] = update_data[c].dt.strftime(cf.DWH_TIME_FORMAT)
    #             update_data[c] = pd.to_datetime(update_data[c],errors='coerce',format="%Y-%m-%d %H:%M:%S")

    #     # update_data.dtypes
    #     # update_data.columns
    #     #
    #     # update_data = update_data[['id', 'creator_id', 'updater_id', 'created_at', 'updated_at', 'full_name', 'is_active', 'email'
    #     #     , 'phone_number', 'birthday', 'avatar', 'last_login', 'password', 'count_sent_free_sms', 'uid_firebase'
    #     #     , 'reason_inactive', 'last_inactive_time', 'user_roles', 'email_notification', 'fresh_desk_id'
    #     #     , 'count_change_domain', 'maximum_free_sms', 'is_anonymous', 'tracking_data', 'insertID', 'loadTime']]

    #     try:
    #         pandas_gbq.to_gbq(update_data,destination_table=table_id,chunksize=50000,if_exists=if_exists,credentials=credentials)
    #     except Exception as e:
    #         print(e)
    #         return False, str(e)

    def get_columns(self, dataset_id, table_id):
        # Get the table schema
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)

        # Extract column names
        column_names = [field.name for field in table.schema]

        return column_names