import io
import math
from typing import Dict, List, Optional, Type
import json
import pandas as pd
from airflow.utils.context import Context
from helper.amis_web_helper import AmisWebHelper
from logging import Logger
from helper.gcp_helper import GCSHelper, BQHelper
import helper.time_helper as TimeHelper  
from helper.mongodb_helper import MongoDBHeler
from helper.redis_helper import RedisHelper
from config import config


class StocksETL:
    def __init__(
            self,
            logger: Logger, 
            project_id: str,
            dataset_id: str,
            table_name: str,
            api_client: AmisWebHelper,  
            gcs: GCSHelper,
            bq: BQHelper,
            redis: RedisHelper,
            mongodb: MongoDBHeler,
            namespace: str,
            vars: Dict,
            context: Context,
        ):
        self.logger = logger
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.amis = api_client
        self.gcs = gcs
        self.bq = bq
        self.redis = redis
        self.mongodb = mongodb
        self.namespace = namespace
        self.vars = vars
        self.context = context

        self.run_config = self.context['dag_run'].conf.get(table_name) if self.context['dag_run'].conf.get(table_name) else self.context['vars'].get(table_name)

        self.dataset_staging_id = config.DATASET_STAGING_ID

    def extract(self):
        """
        Batch process data from Amis
        """
        if not self.run_config.get("extract"):
            self.logger.debug("Skip extract job !")
            return "Success"
        
        self.logger.debug(f"start - Fullload")

        self.logger.debug(f"Get total page")
        summary_stocks = self.amis.get_stocks()
        number_of_items = summary_stocks.get("Total")
        number_of_pages = math.ceil(number_of_items/config.AMIS_WEB_PAGE_LIMIT)

        for page in range(1, number_of_pages+1):

            self.logger.debug(f"Get data {self.table_name} from Amis | page {page}")

            stock_data = self.amis.get_stocks(page=page, load_mode=2)
            list_stocks = stock_data.get("PageData")
    
            if list_stocks:
                self.context['ti'].xcom_push(key=config.NEW_DATA, value=True)
            else:
                self.context['ti'].xcom_push(key=config.NEW_DATA, value=False)
                self.logger.debug("There is no new data. Skip extract job !")
                return "Success"
            
            for stock in list_stocks:
                self.mongodb.update_one(database=config.MONGODB_STAGING, collection=self.table_name, 
                                        contition={"_id": stock.get("stock_id")}, 
                                        update_query={"$set": stock},
                                        upsert=True
                                        )

        return "Success"  

    def transform(self):
        """
        Pull data from GCS, transform data and upload to staging table
        """
        if not self.run_config.get("transform"):
            self.logger.debug("Skip tranform job !")
            return "Success"
        
        if self.context['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA):
            self.logger.debug(f"start - Fullload")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"            

        self.logger.debug("Truncate staging table...")
        # truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        # self.logger.debug(f"Truncate staging table, result {truncate_result}")

        documents = self.mongodb.find(config.MONGODB_STAGING, self.table_name, {}, {"_id": 0})

        df = pd.DataFrame(documents)

        df["created_date"] = df["created_date"].map(lambda i: i.split("+")[0])
        df["modified_date"] = df["modified_date"].map(lambda i: i.split("+")[0])

        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        # print(df.dtypes)
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id)

        # self.mongodb.truncate_collection(database=config.MONGODB_STAGING, collection=self.table_name)

        return "Success"  

    def load(self):
        """
        Execute MERGE statement to upsert (use SCD Type 2) from staging table to curated table and then clear staging table
        """
        if not self.run_config.get("load"):
            self.logger.debug("Skip load job !")
            return "Success"
        
        if self.context['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA):
            self.logger.debug(f"start - Fullload")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"  

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using (
            select *
            from `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`
            where 1=1
            qualify row_number() over(partition by stock_id order by modified_date desc) = 1
        ) s
        on t.stock_id = s.stock_id
        when matched then
        update set 
            t.auto_refno = s.auto_refno,
            t.branch_id = s.branch_id,
            t.branch_name = s.branch_name,
            t.created_by = s.created_by,
            t.created_date = s.created_date,
            t.edit_version = s.edit_version,
            t.excel_row_index = s.excel_row_index,
            t.from_stock_id = s.from_stock_id,
            t.inactive = s.inactive,
            t.inventory_account = s.inventory_account,
            t.isCustomPrimaryKey = s.isCustomPrimaryKey,
            t.isFromProcessBalance = s.isFromProcessBalance,
            t.is_sync_corp = s.is_sync_corp,
            t.is_valid = s.is_valid,
            t.modified_by = s.modified_by,
            t.modified_date = s.modified_date,
            t.pass_edit_version = s.pass_edit_version,
            t.reftype = s.reftype,
            t.reftype_category = s.reftype_category,
            t.state = s.state,
            t.stock_code = s.stock_code,
            t.stock_name = s.stock_name,
            t.to_stock_id = s.to_stock_id
        when not matched then
        insert row
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  