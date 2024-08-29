import io
from typing import Dict, List, Optional, Type
import json
import pandas as pd
from airflow.utils.context import Context
from helper.eshop_helper import EshopHelper
from logging import Logger
from helper.gcp_helper import GCSHelper, BQHelper
import helper.time_helper as TimeHelper  
from helper.mongodb_helper import MongoDBHeler
from helper.redis_helper import RedisHelper
from config import config
import concurrent.futures
from tqdm import tqdm

class InvoiceDetailsETL:
    def __init__(
            self,
            logger: Logger, 
            project_id: str,
            dataset_id: str,
            table_name: str,
            api_client: EshopHelper,  
            gcs: GCSHelper,
            bq: BQHelper,
            redis: RedisHelper,
            mongodb: MongoDBHeler,
            namespace: str,
            params: Dict,
            context: Context,
        ):
        self.logger = logger
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.eshop = api_client
        self.gcs = gcs
        self.bq = bq
        self.redis = redis
        self.mongodb = mongodb
        self.namespace = namespace
        self.params = params
        self.context = context

        self.run_config = self.context['dag_run'].conf.get(table_name) if self.context['dag_run'].conf.get(table_name) else self.context['params'].get(table_name)
        self.start_date = self.context['dag_run'].conf.get('start_date') if self.context['dag_run'].conf.get('start_date') else self.params["start_date"] 
        self.end_date = self.context['dag_run'].conf.get('end_date') if self.context['dag_run'].conf.get('end_date') else self.params["end_date"] 
        self.dataset_staging_id = config.DATASET_STAGING_ID
        self.start_datetime = TimeHelper.get_start_date_format(self.start_date)
        self.end_datetime = TimeHelper.get_end_date_format(self.end_date)

        self.invoices_temp_table = "invoices"

    def extract(self):
        """
        Batch process data from Eshop and upload to GCS
        """
        if not self.run_config.get("extract"):
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")
        self.logger.debug(f"Get data {self.table_name} from Eshop")

        # Get list Invoice will get detail data
        list_invoices = self.mongodb.find(config.MONGODB_TEMP, self.invoices_temp_table, {"GetDetailStatus": False})

        if list_invoices:
            self.context['ti'].xcom_push(key=config.NEW_DATA, value=True)
        else:
            self.context['ti'].xcom_push(key=config.NEW_DATA, value=False)
            self.logger.debug("There is no new data. Skip extract job !")
            return "Success"

        for invoice in list_invoices:
            invoice_id = invoice.get("InvoiceId")
            data = self.eshop.get_invoice_details(invoice_id)
            result = {
                "InvoiceId": invoice_id,
                "CustomerId": data.get("CustomerId"),
                "InvoiceDetails": data.get("InvocieDetails")
            }
            self.mongodb.update_one(
                            database=config.MONGODB_TEMP, 
                            collection=self.invoices_temp_table,
                            contition={"_id": invoice_id},
                            update_query={"$set": {"GetDetailStatus": True}}
                            )
            self.mongodb.insert_one(database=config.MONGODB_STAGING, collection=self.table_name, document={"_id": invoice_id, **result})

        # with concurrent.futures.ThreadPoolExecutor(10) as executor:
        #     with tqdm(total=len(list_invoices), desc="Processing Tasks") as pbar:
        #         for future in executor.map(invoice_details, list_invoices):
        #             pbar.update(1)

        # list_invoice_details = self.mongodb.find(config.MONGODB_STAGING, self.table_name, {}, {"_id": 0})
        
        # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in list_invoice_details)
        # file_name = f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.start_date}/{self.end_date}/{config.PREFIX_JSON_FILE}_{page}.json"
        # self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
        # self.gcs.upload_json(json_string=json_data, file_name=file_name)

        self.mongodb.truncate_collection(database=config.MONGODB_TEMP, collection=self.invoices_temp_table)

        return "Success"  

    def transform(self):
        """
        Pull data from GCS, transform data and upload to staging table
        """
        if not self.run_config.get("transform"):
            self.logger.debug("Skip tranform job !")
            return "Success"
        
        if self.context['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA):
            self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"            

        self.logger.debug("Truncate staging table...")
        truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        self.logger.debug(f"Truncate staging table, result {truncate_result}")

        # blobs  = self.gcs.bucket.list_blobs(prefix=f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.start_date}/{self.end_date}/")
        # blobs = [blob for blob in blobs]

        # df = []
        # for blob in blobs:
        #     json_string = blob.download_as_text()
        #     json_data = io.StringIO(json_string)
        #     df_blob = pd.read_json(json_data, lines=True)
        #     df.append(df_blob)

        # df = pd.concat(df, ignore_index=True)

        documents = self.mongodb.find(config.MONGODB_STAGING, self.table_name, {}, {"_id": 0})

        df = pd.DataFrame(documents)

        df = df.explode("InvoiceDetails")
        df = df.join(pd.json_normalize(df['InvoiceDetails']))

        df = df.drop(columns=['InvoiceDetails', 'EncodeInventoryItemName'])

        df = df[[
            'InvoiceId',
            'InvoiceDetailType',
            'Name',
            'Quantity',
            'UnitName',
            'UnitPrice',
            'Amount',
            'TotalAmount',
            'DiscountAmount',
            'SortOrder',
            'CustomerId',
            'SKU',
        ]]

        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id)

        return "Success"  

    def load(self):
        """
        Execute MERGE statement to upsert (use SCD Type 2) from staging table to curated table and then clear staging table
        """
        if not self.run_config.get("load"):
            self.logger.debug("Skip load job !")
            return "Success"
        
        if self.context['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA):
            self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"   

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using `{self.project_id}.{self.dataset_staging_id}.{self.table_name}` s
        on t.InvoiceId = s.InvoiceId and t.SKU = s.SKU
        when matched then
        update set 
            t.InvoiceDetailType = s.InvoiceDetailType,
            t.ParentId = s.ParentId,
            t.Name = s.Name,
            t.Quantity = s.Quantity,
            t.ItemType = s.ItemType,
            t.UnitId = s.UnitId,
            t.UnitName = s.UnitName,
            t.UnitPrice = s.UnitPrice,
            t.Amount = s.Amount,
            t.TotalAmount = s.TotalAmount,
            t.DiscountAmount = s.DiscountAmount,
            t.SortOrder = s.SortOrder,
            t.CustomerId = s.CustomerId
        when not matched then
        insert row
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  