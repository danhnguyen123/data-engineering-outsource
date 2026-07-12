import io
from typing import Dict, List, Optional, Type
import json
import pandas as pd
from airflow.utils.context import Context
from helper.hubspot_helper import HubspotHelper
from logging import Logger
from helper.gcp_helper import GCSHelper, BQHelper
from helper import time_helper  
from config import config

class HubspotDealsETL:
    def __init__(
            self,
            logger: Logger, 
            project_id: str,
            dataset_id: str,
            table_name: str,
            api_client: HubspotHelper,  
            gcs: GCSHelper,
            bq: BQHelper,
            namespace: str,
            full_load: bool,
            full_date: bool,
            execution_date: str,
            kwargs: Context
        ):
        self.logger = logger
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.hub_spot = api_client
        self.gcs = gcs
        self.bq = bq
        self.namespace = namespace
        self.full_load = full_load
        self.full_date = full_date
        self.execution_date = execution_date
        self.kwargs = kwargs

        self.dataset_staging_id = config.DATASET_STAGING_ID

        if self.full_load:
            self.path_date = config.INIT_DATE
            self.start_timestamp, self.end_timestamp = None, None
        elif self.full_date:
            self.path_date = self.execution_date.replace("-", "/") + config.INIT_HOUR
            self.start_timestamp, self.end_timestamp = time_helper.get_start_end_current_date_epoch(self.execution_date)
        else:
            self.path_date = time_helper.get_format_date_hour_current()
            self.start_timestamp, self.end_timestamp = time_helper.get_start_end_current_date_hour_epoch()

    def extract(self, run=True):
        """
        Batch process data from Hubspot and upload to GCS
        """
        if not run:
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"Run - full load: {self.full_load}, incremental load: start - {self.start_timestamp} | end - {self.end_timestamp}")

        properties = [
            "hs_object_id",
            "dealname",
            "amount",
            "m_phi_u_thu",
            "ngay_bat_dau_hop_dong",
            "ngay_ket_thuc_hop_dong",
            "lop_dang_ky_cloned_",
            "lop_dang_ky_tang_kem",
            ]
        
        if not self.full_load:
            filterGroups = [
                {
                    "filters": [
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "GTE",
                            "value": self.start_timestamp
                        },
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "LTE",
                            "value": self.end_timestamp
                        }     
                    ]
                }
            ]    

        self.logger.debug(f"Get data {self.table_name} from HubSpot")

        if self.full_load:
            results, after = self.hub_spot.list_deals(properties=properties, associations=["contacts"])
            self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=True)
        else:
            results, after = self.hub_spot.search_deals(properties=properties, filterGroups=filterGroups)
            if len(results) > 0:
                self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=True)
            else:
                self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=False)
                self.logger.debug("There is no new data. Skip extract job !")
                return "Success"
            # Get associations contacts
            results = self.hub_spot.merge_association_to_search_result(from_object="deals", to_object="contacts", association_type_id=3, results=results)

        json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

        page_index = 1
        file_name = f"{config.PREFIX_HUBSPOT_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_NAME}_{page_index}.json"
        self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
        self.gcs.upload_json(json_string=json_data, file_name=file_name)

        while after:
            page_index += 1
            self.logger.debug(f"Get data {self.table_name} from HubSpot, page {page_index}, after {after}")

            if self.full_load:
                results, after = self.hub_spot.list_deals(after=after, properties=properties, associations=["contacts"])
            else:
                results, after = self.hub_spot.search_deals(after=after, properties=properties, filterGroups=filterGroups)
                # Get associations contacts
                results = self.hub_spot.merge_association_to_search_result(from_object="deals", to_object="contacts", association_type_id=3, results=results)

            json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

            file_name = f"{config.PREFIX_HUBSPOT_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_NAME}_{page_index}.json"
            self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
            self.gcs.upload_json(json_string=json_data, file_name=file_name)

        return "Success"  

    def transform(self, run=True):
        """
        Pull data from GCS, transform data and upload to staging table
        """
        if not run:
            self.logger.debug("Skip tranform job !")
            return "Success"
        
        if self.kwargs['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA) or self.full_load or self.full_date:
            self.logger.debug(f"Run - full load: {self.full_load}, incremental load: start - {self.start_timestamp} | end - {self.end_timestamp}")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"   
        
        self.logger.debug("Truncate staging table...")
        truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        self.logger.debug(f"Truncate staging table, result {truncate_result}")

        blobs  = self.gcs.bucket.list_blobs(prefix=f"{config.PREFIX_HUBSPOT_BUCKET}/{self.table_name}/{self.path_date}/")
        blobs = [blob for blob in blobs]
        
        df = []
        for blob in blobs:
            json_string = blob.download_as_text()
            json_data = io.StringIO(json_string)
            df_blob = pd.read_json(json_data, lines=True)
            df.append(df_blob)

        df = pd.concat(df, ignore_index=True)

        def get_association(element):
            if isinstance(element, dict):
                return element.get("contacts", {}).get("results", [])[0].get("id")
            else:
                return None

        df["deal_name"] = df["properties"].map(lambda x: x.get("dealname"))
        df["amount"] = df["properties"].map(lambda x: x.get("amount"))
        df["contract_code"] = df["properties"].map(lambda x: x.get("m_phi_u_thu"))
        df["start_date"] = df["properties"].map(lambda x: x.get("ngay_bat_dau_hop_dong"))
        df["end_date"] = df["properties"].map(lambda x: x.get("ngay_ket_thuc_hop_dong"))
        df["main_registration_class"] = df["properties"].map(lambda x: x.get("lop_dang_ky_cloned_"))
        df["free_registration_class"] = df["properties"].map(lambda x: x.get("lop_dang_ky_tang_kem"))
        df["contact_id"] = df["associations"].map(lambda x: get_association(x))
        df["created_datetime"] = df["createdAt"].map(lambda x: time_helper.convert_to_local_datetime_string(x))
        df["updated_datetime"] = df["properties"].map(lambda x: time_helper.convert_to_local_datetime_string(x.get("hs_lastmodifieddate")))

        df = df[["id", "deal_name", "amount", "contract_code", "start_date", "end_date", "main_registration_class", "free_registration_class", "contact_id", "created_datetime", "updated_datetime"]]
        
        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id)

        return "Success"  

    def load(self, run=True):
        """
        Execute MERGE statement to upsert (use SCD Type 2) from staging table to curated table and then clear staging table
        """
        if not run:
            self.logger.debug("Skip load job !")
            return "Success"

        if self.kwargs['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA) or self.full_load or self.full_date:
            self.logger.debug(f"Run - full load: {self.full_load}, incremental load: start - {self.start_timestamp} | end - {self.end_timestamp}")
        else:
            self.logger.debug("There is no new data. Skip load job !")
            return "Success"              

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using `{self.project_id}.{self.dataset_staging_id}.{self.table_name}` s
        on t.id = s.id
        when matched then
        update set 
            t.deal_name = s.deal_name,
            t.amount = s.amount,
            t.contract_code = s.contract_code,
            t.start_date = s.start_date,
            t.main_registration_class = s.main_registration_class,
            t.free_registration_class = s.free_registration_class,
            t.contact_id = s.contact_id,
            t.updated_datetime = s.updated_datetime
        when not matched then
        insert (
            id,
            deal_name, 
            amount, 
            contract_code, 
            start_date, 
            main_registration_class, 
            free_registration_class,
            contact_id,
            created_datetime,
            updated_datetime
            )
        values (
            id,
            deal_name, 
            amount, 
            contract_code, 
            start_date, 
            main_registration_class, 
            free_registration_class,
            contact_id,
            created_datetime,
            updated_datetime
        )
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  