import io
from typing import Dict, List, Optional, Type
import json, time
import pandas as pd
from datetime import datetime, timedelta
from airflow.utils.context import Context
from logging import Logger
from helper.gcp_helper import BQHelper
from helper.redis_helper import RedisHelper
import helper.time_helper as TimeHelper  
from helper.etl_helper import encode_string_to_short_number
from config import config
from helper.pancake_helper import PancakeHelper

class MessagesETL:
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
        self.conversation_redis_key = f"{self.page_id}_conversations"

        conversation_list = self.redis.get_cached_value_for_key_as_list(self.conversation_redis_key)

        if not conversation_list:
            self.logger.debug("No input conversation. Skip extract job !")
            return "Success"    

        df = pd.DataFrame()
        # df_conversation_report = pd.DataFrame()

        for conversation_id in conversation_list:

            df_conversation = pd.DataFrame()
            page = 1
            current_count = None
            filter_previous_commented = None

            while True:
                self.logger.debug(f"Get data {self.table_name} from Pancake | page {page} | conversation {conversation_id} | current_count {(current_count or 0)}")
                
                messages = self.pancake.get_messages(
                    page_access_token=self.page_access_token,
                    page_id=self.page_id,
                    conversation_id=conversation_id,
                    current_count=current_count
                )

                if not messages:
                    break

                if page == 1:
                    filter_previous_commented = (pd.to_datetime(messages[0].get("inserted_at")) - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)

                    # if conversation_report:
                    #     df_conversation_report = pd.concat([df_conversation_report, pd.DataFrame(conversation_report)], ignore_index=True)

                df_conversation = pd.concat([df_conversation, pd.DataFrame(messages)], ignore_index=True)

                last_commented_at = pd.to_datetime(messages[-1].get("inserted_at"))

                # print(f"last_commented_at: {last_commented_at}")
                # print(f"filter_previous_commented: {filter_previous_commented}")

                if last_commented_at < filter_previous_commented:
                    break

                current_count = (current_count or 0) + len(messages)
                page += 1

                time.sleep(0.5)

            # Filter message
            df_conversation = df_conversation[df_conversation['inserted_at'].apply(lambda x: pd.to_datetime(x) >= filter_previous_commented)]
            df = pd.concat([df, df_conversation], ignore_index=True)

        if df.empty:
            self.logger.debug(f"The DataFrame has no data rows. Skip")
            return "Success"
        
        self.logger.debug(f"The df DataFrame has {len(df)} rows.")
        # self.logger.debug(f"The df_conversation_report DataFrame has {len(df_conversation_report)} rows.")

        # Transform

        # print(df["from"])

        expected_columns = [
            'id',
            'seen',
            'from',
            'inserted_at',
            'page_id',
            'conversation_id',
            'attachments',
            'original_message'
        ]

        # self.logger.debug(f"Columns of df 1: {df.columns.tolist()}")

        available_columns = [col for col in expected_columns if col in df.columns]

        df = df[available_columns]

        df['id'] = df['id'].apply(encode_string_to_short_number)

        # self.logger.debug(f"Columns of df 2: {df.columns.tolist()}")

        df["from"] = df["from"].apply(lambda x: {"id": x.get("admin_id", x.get("id")), "name": x.get("admin_name", x.get("name")), "admin": True if x.get("admin_id") else False} if isinstance(x, dict) else None)

        df['inserted_at'] = pd.to_datetime(df['inserted_at'], errors='coerce').dt.floor('S')

        df["attachments"] = df["from"].apply(lambda x: True if x else False)

        df = df.rename(columns={'from': 'sender'})

        # Config for load process
        self.table_cols = list(df.columns)

        # Load staging table
        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id, load_method="load_parquet")

        self.redis.remove_cached_value_for_key(self.conversation_redis_key)

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
    
    # def extract_tag_texts(self, tag_list):
    #     if isinstance(tag_list, list):
    #         return [tag['text'] for tag in tag_list if isinstance(tag, dict) and 'text' in tag]
    #     return [] 
    
    # def extract_recent_phone_numbers(self, recent_phone_numbers):
    #     if isinstance(recent_phone_numbers, list):
    #         return [phone['phone_number'] for phone in recent_phone_numbers if isinstance(phone, dict) and 'phone_number' in phone]
    #     return [] 