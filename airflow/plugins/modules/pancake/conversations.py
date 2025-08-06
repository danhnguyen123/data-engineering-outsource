import io
from typing import Dict, List, Optional, Type
import json
import pandas as pd
from airflow.utils.context import Context
from logging import Logger
from helper.gcp_helper import BQHelper
from helper.redis_helper import RedisHelper
import helper.time_helper as TimeHelper  
from config import config
from helper.pancake_helper import PancakeHelper

class ConversationsETL:
    def __init__(
            self,
            logger: Logger, 
            project_id: str,
            dataset_id: str,
            table_name: str,
            pancake: PancakeHelper,  
            bq: BQHelper,
            redis: RedisHelper,
            namespace: str,
            vars: Dict,
            context: Context,
        ):
        self.logger = logger
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.pancake = pancake
        self.bq = bq
        self.redis = redis
        self.namespace = namespace
        self.vars = vars
        self.context = context

        self.run_config = self.context['dag_run'].conf.get(table_name) if self.context['dag_run'].conf.get(table_name) else self.context['params'].get(table_name)
        self.start_date = self.context['dag_run'].conf.get('start_date') if self.context['dag_run'].conf.get('start_date') else self.vars["start_date"] 
        self.end_date = self.context['dag_run'].conf.get('end_date') if self.context['dag_run'].conf.get('end_date') else self.vars["end_date"] 
        self.dataset_staging_id = config.DATASET_STAGING_ID
        self.start_datetime = TimeHelper.get_unix_timestamp(TimeHelper.get_start_datetime_of_date(self.start_date))
        self.end_datetime = TimeHelper.get_unix_timestamp(TimeHelper.get_end_datetime_of_date(self.end_date))

        self.table_cols = None

    def extract(self):
        """
        Batch process data from Pancake and upload to GCS
        """
        if not self.run_config.get("extract"):
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")

        self.page_access_token = self.vars.get("page").get("page_access_token")
        self.page_id = self.vars.get("page").get("page_id")
        self.platform = self.vars.get("page").get("platform")
        self.page_name = self.vars.get("page").get("name")
        self.pancake_url = self.vars.get("page").get("pancake_url")
        self.conversation_redis_key = f"{self.page_id}_conversations"

        page = 1

        df = pd.DataFrame()

        last_conversation_id = None

        while True:
            self.logger.debug(f"Get data {self.table_name} from Pancake | page {page}")
            
            results = self.pancake.get_conversations(
                page_access_token=self.page_access_token,
                page_id=self.page_id,
                last_conversation_id=last_conversation_id,
                since=self.start_datetime,
                until=self.end_datetime
            )

            if not results:
                self.logger.debug(f"Emtry Result: {results}")
                break
        
            df = pd.concat([df, pd.DataFrame(results)], ignore_index=True)
            last_conversation_id = results[-1].get("id")
            page += 1

        if df.empty:
            self.logger.debug(f"The DataFrame has no data rows. Skip")
            return "Success"
        
        self.logger.debug(f"The DataFrame has {len(df)} rows.")

        # Transform

        expected_columns = [
            'id',
            'type',
            'tags',
            'seen',
            'from',
            'inserted_at',
            'updated_at',
            'message_count',
            'page_id',
            'last_sent_by',
            'recent_phone_numbers',
            'page_customer',
            'ad_ids',
        ]

        available_columns = [col for col in expected_columns if col in df.columns]

        df = df[available_columns]

        df['tags'] = df['tags'].apply(self.extract_tag_texts)
        df["from"] = df["from"].apply(lambda x: {"id": x.get("id"), "name": x.get("name")} if isinstance(x, dict) else None)

        df['inserted_at'] = pd.to_datetime(df['inserted_at'], errors='coerce').dt.floor('S')
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce').dt.floor('S')

        df["last_sent_by"] = df["last_sent_by"].apply(lambda x: {"admin_id": x.get("admin_id"), "admin_name": x.get("admin_name")} if isinstance(x, dict) else None)
        df['recent_phone_numbers'] = df['recent_phone_numbers'].apply(self.extract_recent_phone_numbers)

        df["page_customer"] = df["page_customer"].apply(
            lambda x: {"id": x.get("id"), 
                       "name": x.get("name"),
                       "customer_id": x.get("customer_id"), 
                       "psid": x.get("psid"),
                       "global_id": x.get("global_id"),
                       } 
            if x else None
            )

        df["platform"] = self.platform
        df["page_name"] = self.page_name
        df["link"] = f"{self.pancake_url}?c_id=" + df["id"]

        df = df.rename(columns={'from': 'customers'})

        # print(df['tags'])

        # Config for load process
        self.table_cols = list(df.columns)

        # Load staging table
        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id, load_method="load_parquet")

        # Cache list conversation
        conversation_list = df['id'].tolist()
        self.redis.remove_cached_value_for_key(self.conversation_redis_key)
        self.redis.put_cached_value_for_as_list(self.conversation_redis_key, conversation_list)

        return "Success"  

    def load(self):
        """
        Execute MERGE statement to upsert (use SCD Type 2) from staging table to curated table and then clear staging table
        """
        if not self.run_config.get("load"):
            self.logger.debug("Skip load job !")
            return "Success"
        
        identifier_cols = ['id']
        
        table_cols = self.table_cols if self.table_cols else self.bq.get_columns(dataset_id=self.dataset_staging_id, table_id=self.table_name)
        
        on_clause = " and ".join([f"target.{col}=source.{col}" for col in identifier_cols])

        update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in table_cols if col not in identifier_cols])
        insert_values_clause = ", ".join([f"source.{col}" for col in table_cols])

        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.{self.table_name}` AS target
        USING `{self.project_id}.{self.dataset_staging_id}.{self.table_name}` AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN 
            INSERT ({", ".join(table_cols)}) 
            VALUES ({insert_values_clause}) 
        """

        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(f"Job ID: {results.job_id}")
 
        self.logger.debug("Truncate staging table...")
        self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")

        return "Success"  
    
    def extract_tag_texts(self, tag_list):
        if isinstance(tag_list, list):
            return [str(tag.get('text')) for tag in tag_list if isinstance(tag, dict) and 'text' in tag]
        return [] 
    
    def extract_recent_phone_numbers(self, recent_phone_numbers):
        if isinstance(recent_phone_numbers, list):
            return [phone['phone_number'] for phone in recent_phone_numbers if isinstance(phone, dict) and 'phone_number' in phone]
        return [] 