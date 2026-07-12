from typing import Dict, List, Optional, Type
import pandas as pd
from airflow.utils.context import Context
from helper.dahahi_helper import DahahiHelper
from logging import Logger
from helper.gcp_helper import BQHelper
from config import config

class DahahiEmployeesETL:
    def __init__(
            self,
            logger: Logger,
            project_id: str,
            dataset_id: str,
            table_name: str,
            api_client: DahahiHelper,
            bq: BQHelper,
            namespace: str,
            kwargs: Context
        ):
        self.logger = logger
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.dahahi = api_client
        self.bq = bq
        self.namespace = namespace
        self.kwargs = kwargs

        self.dataset_staging_id = config.DATASET_STAGING_ID
        self.page_size = config.DAHAHI_PAGE_SIZE

    def extract(self, run=True):
        """
        Fetch the full employee list from Dahahi (GetEmployeeList), transform in memory
        and load to the staging table (no GCS).
        """
        if not run:
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"Get data {self.table_name} from Dahahi")

        records = []
        page_index = 1
        while True:
            results = self.dahahi.get_employee_list(page_size=self.page_size, page_index=page_index)
            if not results:
                break
            records.extend(results)
            self.logger.debug(f"Get data {self.table_name} from Dahahi, page {page_index}, got {len(results)}")
            if len(results) < self.page_size:
                break
            page_index += 1

        if not records:
            self.logger.debug("There is no data. Skip extract job !")
            return "Success"

        self.logger.debug(f"Transform {len(records)} rows of {self.table_name}")

        df = pd.DataFrame.from_records(records)

        # Avatar: fall back to FaceUrl when Avatar is null/empty, then build a full URL
        # (Dahahi returns a relative path like "/Images/Snap/.../x.jpg")
        base_url = config.DAHAHI_BASE_URL.rstrip("/")

        def _avatar_url(row):
            raw = row["FaceUrl"] if (pd.isna(row["Avatar"]) or row["Avatar"] == "") else row["Avatar"]
            if pd.isna(raw) or raw == "":
                return None
            if str(raw).startswith("http"):
                return raw
            return f"{base_url}{raw}"

        df["Avatar"] = df.apply(_avatar_url, axis=1)

        df = df[[
            "EmployeeCode",
            "Name",
            "Mobile",
            "Email",
            "Address",
            "Gender",
            "CreatedBy",
            "CreatedDate",
            "Avatar",
        ]]

        self.logger.debug("Truncate staging table...")
        truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        self.logger.debug(f"Truncate staging table, result {truncate_result}")

        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id)

        return "Success"

    def load(self, run=True):
        """
        Execute MERGE statement to upsert from staging table to curated table (key: EmployeeCode)
        and then clear staging table.
        """
        if not run:
            self.logger.debug("Skip load job !")
            return "Success"

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using `{self.project_id}.{self.dataset_staging_id}.{self.table_name}` s
        on t.EmployeeCode = s.EmployeeCode
        when matched then
        update set
            t.Name = s.Name,
            t.Mobile = s.Mobile,
            t.Email = s.Email,
            t.Address = s.Address,
            t.Gender = s.Gender,
            t.CreatedBy = s.CreatedBy,
            t.CreatedDate = s.CreatedDate,
            t.Avatar = s.Avatar
        when not matched then
        insert (
            EmployeeCode,
            Name,
            Mobile,
            Email,
            Address,
            Gender,
            CreatedBy,
            CreatedDate,
            Avatar
            )
        values (
            EmployeeCode,
            Name,
            Mobile,
            Email,
            Address,
            Gender,
            CreatedBy,
            CreatedDate,
            Avatar
        )
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        self.logger.debug("Truncate staging table...")
        self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")

        return "Success"
