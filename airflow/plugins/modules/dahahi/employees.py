from typing import Dict, List, Optional, Type
import re
import pandas as pd
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound
from helper.dahahi_helper import DahahiHelper
from logging import Logger
from helper.gcp_helper import BQHelper
from helper import time_helper
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

        # Per-run temporary table (created fresh on extract, auto-expiring) - no persistent staging table.
        # extract & load are separate tasks, so the name is derived from run_id so both agree.
        run_suffix = re.sub(r'[^0-9a-zA-Z]+', '_', str(self.kwargs['dag_run'].run_id)).strip('_')
        self.temp_table_name = f"{self.table_name}__tmp_{run_suffix}"
        self.temp_table_id = f"{self.project_id}.{self.dataset_staging_id}.{self.temp_table_name}"

    def extract(self, run=True):
        """
        Fetch the full employee list from Dahahi (GetEmployeeList), transform in memory
        and load into a per-run temp table (no persistent staging table).
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
        df["ingested_at"] = time_helper.get_datetime_local()

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
            "ingested_at",
        ]]

        self.logger.debug(f"The DataFrame has {len(df)} rows. Load into temp table {self.temp_table_id}")
        # Load into a per-run temp table (created fresh, replaces any leftover) - no persistent staging table
        self.bq.bq_append(
            update_data=df,
            table_name=self.temp_table_name,
            dataset_id=self.dataset_staging_id,
            if_exists='replace',
        )
        # Safety net: auto-expire the temp table so it is cleaned up even if the load task never runs
        self.bq.execute(
            f"ALTER TABLE `{self.temp_table_id}` "
            "SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))"
        )

        return "Success"

    def load(self, run=True):
        """
        MERGE the per-run temp table into the destination table (key: EmployeeCode).
        The temp table is left to auto-expire (no explicit DROP).
        """
        if not run:
            self.logger.debug("Skip load job !")
            return "Success"

        # extract may have produced no data -> no temp table to merge
        try:
            self.bq.client.get_table(self.temp_table_id)
        except NotFound:
            self.logger.debug(f"Temp table {self.temp_table_id} not found (no data extracted). Skip load !")
            return "Success"

        # Build the MERGE dynamically from the temp table columns (same approach as ConversationsETL),
        # keeping the latest row per EmployeeCode via QUALIFY on ingested_at.
        identifier_cols = ['EmployeeCode']
        table_cols = self.bq.get_columns(dataset_id=self.dataset_staging_id, table_id=self.temp_table_name)

        partition_cols = ", ".join(identifier_cols)
        on_clause = " and ".join([f"target.{col} = source.{col}" for col in identifier_cols])
        update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in table_cols if col not in identifier_cols])
        insert_cols_clause = ", ".join(table_cols)
        insert_values_clause = ", ".join([f"source.{col}" for col in table_cols])

        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.{self.table_name}` AS target
        USING (
            SELECT *
            FROM `{self.temp_table_id}`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY ingested_at DESC) = 1
        ) AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols_clause})
            VALUES ({insert_values_clause})
        """
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        # Temp table is left to auto-expire (expiration set during extract); no explicit DROP.
        return "Success"
