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


class InventoryItemsETL:
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
        self.dataset_staging_id = config.DATASET_STAGING_ID
        self.path_date = self.start_date if self.start_date else "0000-00-00"

    def extract(self):
        """
        Batch process data from Eshop and upload to GCS
        """
        if not self.run_config.get("extract"):
            self.logger.debug("Skip extract job !")
            return "Success"
        
        if self.start_date:
            self.logger.debug(f"start - {self.start_date}")
        else:
            self.logger.debug(f"start - Fullload")

        self.logger.debug(f"Get last page")
        page =  self.redis.get_cached_value_for_key(config.ESHOP_INVENTORY_LATEST_PAGE_REDIS)
        page = int(page) if page else 1

        self.logger.debug(f"Get data {self.table_name} from Eshop | page {page}")

        results = self.eshop.get_inventory_items(page=page, last_sync_date=self.start_date)
    
        if results:
            self.context['ti'].xcom_push(key=config.NEW_DATA, value=True)
        else:
            self.context['ti'].xcom_push(key=config.NEW_DATA, value=False)
            self.logger.debug("There is no new data. Skip extract job !")
            return "Success"

        # data_inventory_items = []
        # for inventory_items in results:
        #     data_inventory_items.append({"_id": inventory_items.get("Id"), **inventory_items})

        # self.mongodb.insert_many(database=config.MONGODB_STAGING, collection=self.table_name, list_document=data_inventory_items)
        self.mongodb.insert_many(database=config.MONGODB_STAGING, collection=self.table_name, list_document=results)

        # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)
        # file_name = f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_FILE}_{page}.json"
        # self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
        # self.gcs.upload_json(json_string=json_data, file_name=file_name)

        self.redis.put_cached_value_for_key(config.ESHOP_INVENTORY_LATEST_PAGE_REDIS, page, 300)

        while len(results) == config.ESHOP_PAGE_LIMIT:
            page += 1
            self.logger.debug(f"Get data {self.table_name} from Eshop {page}")

            results = self.eshop.get_inventory_items(page=page, last_sync_date=self.start_date)

            if results:
                # data_inventory_items = []
                # for inventory_items in results:
                #     data_inventory_items.append({"_id": inventory_items.get("Id"), **inventory_items})

                # self.mongodb.insert_many(database=config.MONGODB_STAGING, collection=self.table_name, list_document=data_inventory_items)
                self.mongodb.insert_many(database=config.MONGODB_STAGING, collection=self.table_name, list_document=results)

                # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)
                # file_name = f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_FILE}_{page}.json"
                # self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
                # self.gcs.upload_json(json_string=json_data, file_name=file_name)

                self.redis.put_cached_value_for_key(config.ESHOP_INVENTORY_LATEST_PAGE_REDIS, page, 300)

        self.redis.remove_cached_value_for_key(config.ESHOP_INVENTORY_LATEST_PAGE_REDIS)

        return "Success"  

    def transform(self):
        """
        Pull data from GCS, transform data and upload to staging table
        """
        if not self.run_config.get("transform"):
            self.logger.debug("Skip tranform job !")
            return "Success"
        
        if self.context['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA):
            if self.start_date:
                self.logger.debug(f"start - {self.start_date}")
            else:
                self.logger.debug(f"start - Full-load")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"            

        self.logger.debug("Truncate staging table...")
        truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        self.logger.debug(f"Truncate staging table, result {truncate_result}")

        # blobs  = self.gcs.bucket.list_blobs(prefix=f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.path_date}/")
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

        df = df.drop(columns=['BranchId', 'Picture', 'ListPictureUrl'])

        df = df.rename(columns={'SellingPrice': 'SellingPriceBK'})

        df = df.explode("Inventories")

        df = df.join(pd.json_normalize(df['Inventories']))

        df["SellingPrice"] = df.apply(lambda row: row['SellingPrice'] if row['SellingPrice'] else row['SellingPriceBK'], axis=1)

        df = df.drop(columns=['SellingPriceBK', 'Inventories'])

        df["Color"] = df["Color"].map(lambda i: i if i else None)
        df["Size"] = df["Size"].map(lambda i: i if i else None)
        df["Description"] = df["Description"].map(lambda i: i if i else None)

        df["ModifiedDate"] = df["ModifiedDate"].map(lambda i: i.split("+")[0])

        df = df[[
            'Id',
            'Code',
            'Name',
            'ItemType',
            'ItemCategoryId',
            'ItemCategoryName',
            'Barcode',
            'CostPrice',
            'Color',
            'Size',
            'Description',
            'IsItem',
            'Inactive',
            'UnitId',
            'UnitName',
            'AvgUnitPrice',
            'ProductId',
            'ProductCode',
            'ProductName',
            'BranchId',
            'BranchName',
            'SellingPrice',
            'OnHand',
            'Ordered',
            'PreOrdered',
            'ModifiedDate',
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
            if self.start_date:
                self.logger.debug(f"start - {self.start_date}")
            else:
                self.logger.debug(f"start - Full-load")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"  

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using (
            select *
            from `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`
            where 1=1
            qualify row_number() over(partition by Id, BranchId order by ModifiedDate desc) = 1
        ) s
        on t.Id = s.Id and t.BranchId = s.BranchId
        when matched then
        update set 
            t.Code = s.Code,
            t.Name = s.Name,
            t.ItemType = s.ItemType,
            t.ItemCategoryId = s.ItemCategoryId,
            t.ItemCategoryName = s.ItemCategoryName,
            t.Barcode = s.Barcode,
            t.CostPrice = s.CostPrice,
            t.Color = s.Color,
            t.Size = s.Size,
            t.Description = s.Description,
            t.IsItem = s.IsItem,
            t.Inactive = s.Inactive,
            t.UnitId = s.UnitId,
            t.UnitName = s.UnitName,
            t.AvgUnitPrice = s.AvgUnitPrice,
            t.ProductId = s.ProductId,
            t.ProductCode = s.ProductCode,
            t.ProductName = s.ProductName,
            t.BranchName = s.BranchName,
            t.SellingPrice = s.SellingPrice,
            t.OnHand = s.OnHand,
            t.Ordered = s.Ordered,
            t.PreOrdered = s.PreOrdered,
            t.ModifiedDate = s.ModifiedDate
        when not matched then
        insert row
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  