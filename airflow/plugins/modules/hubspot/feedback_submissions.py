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

class HubspotFeedbacksETL:
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
            'dau_khoa___danh_gia_quy_trinh___sap_xep',
            'dau_khoa___danh_gia_cac_yeu_to_dich_vu',
            'dau_khoa___thiet_ke_chuong_trinh_va_hinh_thuc_hoc_tap',
            'dau_khoa___danh_gia_giang_vien',
            'dau_khoa___diem_an_tuong',
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
            results, after = self.hub_spot.list_feedback_submissions(properties=properties, associations=["contacts"])
            self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=True)
        else:
            results, after = self.hub_spot.search_feedback_submissions(properties=properties, filterGroups=filterGroups)
            if len(results) > 0:
                self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=True)
            else:
                self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=False)
                self.logger.debug("There is no new data. Skip extract job !")
                return "Success"
            # Get associations contacts
            results = self.hub_spot.merge_association_to_search_result(from_object="feedback_submissions", to_object="contacts", association_type_id=98, results=results)

        json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

        page_index = 1
        file_name = f"{config.PREFIX_HUBSPOT_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_NAME}_{page_index}.json"
        self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
        self.gcs.upload_json(json_string=json_data, file_name=file_name)

        while after:
            page_index += 1
            self.logger.debug(f"Get data {self.table_name} from HubSpot, page {page_index}, after {after}")

            if self.full_load:
                results, after = self.hub_spot.list_feedback_submissions(after=after, properties=properties, associations=["contacts"])
            else:
                results, after = self.hub_spot.search_feedback_submissions(after=after, properties=properties, filterGroups=filterGroups)
                # Get associations contacts
                results = self.hub_spot.merge_association_to_search_result(from_object="feedback_submissions", to_object="contacts", association_type_id=98, results=results)

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

        df["danh_gia_quy_trinh_sap_xep"] = df["properties"].map(lambda x: x.get("dau_khoa___danh_gia_quy_trinh___sap_xep"))
        df["danh_gia_chung"] = df["properties"].map(lambda x: x.get("dau_khoa___danh_gia_cac_yeu_to_dich_vu"))
        df["thiet_ke_chuong_trinh"] = df["properties"].map(lambda x: x.get("dau_khoa___thiet_ke_chuong_trinh_va_hinh_thuc_hoc_tap"))
        df["danh_gia_giang_vien"] = df["properties"].map(lambda x: x.get("dau_khoa___danh_gia_giang_vien"))
        df["diem_an_tuong"] = df["properties"].map(lambda x: x.get("dau_khoa___diem_an_tuong"))

        df["contact_id"] = df["associations"].map(lambda x: get_association(x))
        df["created_datetime"] = df["createdAt"].map(lambda x: time_helper.convert_to_local_datetime_string(x))
        df["updated_datetime"] = df["properties"].map(lambda x: time_helper.convert_to_local_datetime_string(x.get("hs_lastmodifieddate")))

        df = df[["id", "danh_gia_quy_trinh_sap_xep", "danh_gia_chung", "thiet_ke_chuong_trinh", "danh_gia_giang_vien", "diem_an_tuong", "contact_id", "created_datetime", "updated_datetime"]]
        
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
            t.danh_gia_quy_trinh_sap_xep = s.danh_gia_quy_trinh_sap_xep,
            t.danh_gia_chung = s.danh_gia_chung,
            t.thiet_ke_chuong_trinh = s.thiet_ke_chuong_trinh,
            t.danh_gia_giang_vien = s.danh_gia_giang_vien,
            t.diem_an_tuong = s.diem_an_tuong,
            t.contact_id = s.contact_id,
            t.updated_datetime = s.updated_datetime
        when not matched then
        insert (
            id,
            danh_gia_quy_trinh_sap_xep,
            danh_gia_chung,
            thiet_ke_chuong_trinh,
            danh_gia_giang_vien,
            diem_an_tuong,
            contact_id,
            created_datetime,
            updated_datetime
            )
        values (
            id,       
            danh_gia_quy_trinh_sap_xep,
            danh_gia_chung,
            thiet_ke_chuong_trinh,
            danh_gia_giang_vien,
            diem_an_tuong,
            contact_id,
            created_datetime,
            updated_datetime
        )
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  