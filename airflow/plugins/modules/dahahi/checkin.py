import io
from typing import Dict, List, Optional, Type
import json
import pandas as pd
from airflow.utils.context import Context
from helper.dahahi_helper import DahahiHelper
from logging import Logger
from helper.gcp_helper import GCSHelper, BQHelper
from helper import time_helper  
from config import config

class DahahiCheckinETL:
    def __init__(
            self,
            logger: Logger, 
            project_id: str,
            dataset_id: str,
            table_name: str,
            api_client: DahahiHelper,  
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
        self.dahahi = api_client
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
            self.start_timestamp, self.end_timestamp = time_helper.get_start_end_current_date_format(self.execution_date)
        else:
            self.path_date = time_helper.get_format_date_hour_current()
            self.start_timestamp, self.end_timestamp = time_helper.get_start_end_current_date_hour_format()

    def extract(self, run=True):
        """
        Batch process data from Dahahi and upload to GCS
        """
        if not run:
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"Run - full load: {self.full_load}, incremental load: start - {self.start_timestamp} | end - {self.end_timestamp}")

        
        self.logger.debug(f"Get data {self.table_name} from Dahahi")

        page_index = 1

        if self.full_load:
            results = self.dahahi.get_checkin_history(page_index=page_index)
            self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=True)
        else:
            results = self.dahahi.get_checkin_history(page_index=page_index, from_time_string=self.start_timestamp, to_time_string=self.end_timestamp)
            if len(results) > 0:
                self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=True)
            else:
                self.kwargs['ti'].xcom_push(key=config.NEW_DATA, value=False)
                self.logger.debug("There is no new data. Skip extract job !")
                return "Success"

        json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)
        file_name = f"{config.PREFIX_DAHAHI_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_NAME}_{page_index}.json"
        self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
        self.gcs.upload_json(json_string=json_data, file_name=file_name)

        print(results)

        while results:
            page_index += 1
            self.logger.debug(f"Get data {self.table_name} from HubSpot, page {page_index}")

            if self.full_load:
                results = self.dahahi.get_checkin_history(page_index=page_index)
            else:
                results = self.dahahi.get_checkin_history(page_index=page_index, from_time_string=self.start_timestamp, to_time_string=self.end_timestamp)

            if results:
                json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)
                file_name = f"{config.PREFIX_DAHAHI_BUCKET}/{self.table_name}/{self.path_date}/{config.PREFIX_JSON_NAME}_{page_index}.json"
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

        blobs  = self.gcs.bucket.list_blobs(prefix=f"{config.PREFIX_DAHAHI_BUCKET}/{self.table_name}/{self.path_date}/")
        blobs = [blob for blob in blobs]

        df = []
        for blob in blobs:
            json_string = blob.download_as_text()
            json_data = io.StringIO(json_string)
            df_blob = pd.read_json(json_data, lines=True)
            df.append(df_blob)

        df = pd.concat(df, ignore_index=True)
        df["face_id"] = df["FaceId"].map(lambda x: int(x))
        df["face_person_id"] = df["FacePersonId"]
        df["employee_code"] = df["EmployeeCode"]
        df["employee_id"] = df["EmployeeIdStr"]
        df["employee_name"] = df["EmployeeName"]
        df["checkin_datetime"] = df["CheckinTimeStr"].map(lambda x: time_helper.convert_formated_to_local_datetime_string(x))

        df = df[["face_person_id", "face_id", "employee_code", "employee_id", "employee_name", "checkin_datetime"]]
        
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
        on t.face_id = s.face_id and t.face_person_id = s.face_person_id and t.checkin_datetime = s.checkin_datetime
        when matched then
        update set 
            t.face_id = s.face_id,
            t.face_person_id = s.face_person_id,
            t.employee_code = s.employee_code,
            t.employee_id = s.employee_id,
            t.employee_name = s.employee_name,
            t.checkin_datetime = s.checkin_datetime
        when not matched then
        insert (
            face_id,
            face_person_id, 
            employee_code, 
            employee_id, 
            employee_name, 
            checkin_datetime, 
            inserted_datetime
            )
        values (
            face_id,
            face_person_id, 
            employee_code, 
            employee_id, 
            employee_name, 
            checkin_datetime, 
            CURRENT_DATETIME("+7")
        )
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  