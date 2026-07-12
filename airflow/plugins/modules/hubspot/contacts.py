from typing import Dict, List, Optional, Type
import json
import pandas as pd
from airflow.utils.context import Context
from helper.hubspot_helper import HubspotHelper
from logging import Logger
from helper.gcp_helper import BQHelper
from helper import time_helper
from config import config

class HubspotContactsETL:
    def __init__(
            self,
            logger: Logger,
            project_id: str,
            dataset_id: str,
            table_name: str,
            api_client: HubspotHelper,
            bq: BQHelper,
            namespace: str,
            kwargs: Context
        ):
        self.logger = logger
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.hub_spot = api_client
        self.bq = bq
        self.namespace = namespace
        self.kwargs = kwargs

        self.dataset_staging_id = config.DATASET_STAGING_ID

        conf = self.kwargs['dag_run'].conf
        self.start_date = conf.get('start_date')
        self.end_date = conf.get('end_date')

        if self.start_date and self.end_date:
            # Manual backfill over an explicit date range:
            # start of start_date -> end of end_date (UTC+7, epoch ms)
            self.start_timestamp = time_helper.get_start_end_current_date_epoch(self.start_date)[0]
            self.end_timestamp = time_helper.get_start_end_current_date_epoch(self.end_date)[1]
        else:
            # Scheduled daily run: full previous day (start of yesterday -> end of yesterday)
            yesterday = time_helper.get_start_delta_date_format(delta=1)
            self.start_timestamp, self.end_timestamp = time_helper.get_start_end_current_date_epoch(yesterday)

    def extract(self, run=True):
        """
        Batch process data from Hubspot, transform in memory and load to staging table (no GCS)
        """
        if not run:
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"Run - search contacts: start - {self.start_timestamp} | end - {self.end_timestamp}")

        properties = [
            "hs_object_id",
            "firstname",
            "email",
            "phone",
            "d_o_b",
            "chuyen_nganh_hoc",
            "truongdaihoc",
            "loai_hinh_cong_ty",
            "vi_tri_cong_tac",
            "cap_bac",
            "chuong_trinh_hoc",
            "b_n_thu_c_truong_nao",
            "lifecyclestage",
            "lastmodifieddate"
            ]

        # Which date property to filter on:
        #   - scheduled runs   -> "lastmodifieddate" (incremental by last update)
        #   - manual backfill  -> "createdate" (fewer records per window, avoids the
        #     10k/search limit; trigger with conf {"date_property": "createdate", ...})
        date_property = (
            self.kwargs['dag_run'].conf.get('date_property')
            or self.kwargs['params'].get('date_property')
            or 'lastmodifieddate'
        )

        # HubSpot search: filters within a group are AND-ed, groups are OR-ed.
        # We want: (start <= date_property <= end) AND (email HAS_PROPERTY OR phone HAS_PROPERTY)
        # so the date range is distributed into one group per HAS_PROPERTY condition.
        date_filters = [
            {
                "propertyName": date_property,
                "operator": "GTE",
                "value": self.start_timestamp
            },
            {
                "propertyName": date_property,
                "operator": "LTE",
                "value": self.end_timestamp
            }
        ]
        filterGroups = [
            {
                "filters": date_filters + [
                    {"propertyName": "email", "operator": "HAS_PROPERTY"}
                ]
            },
            {
                "filters": date_filters + [
                    {"propertyName": "phone", "operator": "HAS_PROPERTY"}
                ]
            }
        ]

        self.logger.debug(f"Get data {self.table_name} from HubSpot")

        results, after = self.hub_spot.search_contacts(properties=properties, filterGroups=filterGroups)

        records = list(results)

        page_index = 1
        while after:
            page_index += 1
            self.logger.debug(f"Get data {self.table_name} from HubSpot, page {page_index}, after {after}")

            results, after = self.hub_spot.search_contacts(after=after, properties=properties, filterGroups=filterGroups)

            records.extend(results)

        if not records:
            self.logger.debug("There is no new data. Skip extract job !")
            return "Success"

        self.logger.debug(f"Transform {len(records)} rows of {self.table_name}")

        df = pd.DataFrame.from_records(records)
        df["hubspot_link"] = [f'https://app.hubspot.com/contacts/{config.HUBSPOT_ACCOUNT_ID}/contact/{i}' for i in df["id"]]
        df["full_name"] = df["properties"].map(lambda x: x.get("firstname"))
        df["email"] = df["properties"].map(lambda x: x.get("email"))
        df["phone"] = df["properties"].map(lambda x: x.get("phone"))
        df["date_of_birth"] = df["properties"].map(lambda x: x.get("d_o_b"))
        df["school"] = df["properties"].map(lambda x: x.get("truongdaihoc"))
        df["major"] = df["properties"].map(lambda x: x.get("chuyen_nganh_hoc"))
        df["company_type"] = df["properties"].map(lambda x: x.get("loai_hinh_cong_ty"))
        df["position"] = df["properties"].map(lambda x: x.get("vi_tri_cong_tac"))
        df["level"] = df["properties"].map(lambda x: x.get("cap_bac"))
        df["study_area"] = df["properties"].map(lambda x: x.get("chuong_trinh_hoc"))
        df["area"] = df["properties"].map(lambda x: x.get("b_n_thu_c_truong_nao"))
        df["lifecycle_stage"] = df["properties"].map(lambda x: x.get("lifecyclestage"))
        df["created_datetime"] = df["createdAt"].map(lambda x: time_helper.convert_to_local_datetime_string(x))
        df["updated_datetime"] = df["properties"].map(lambda x: time_helper.convert_to_local_datetime_string(x.get("lastmodifieddate")))

        df = df[["id", "hubspot_link", "full_name", "email", "phone", "date_of_birth", "school", "major", "company_type", "position", "level", "study_area", "area", "lifecycle_stage", "created_datetime", "updated_datetime"]]

        self.logger.debug("Truncate staging table...")
        truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        self.logger.debug(f"Truncate staging table, result {truncate_result}")

        self.logger.debug(f"The DataFrame has {len(df)} rows.")
        self.bq.bq_append(update_data=df, table_name=self.table_name, dataset_id=self.dataset_staging_id)

        return "Success"

    def load(self, run=True):
        """
        Execute MERGE statement to upsert from staging table to curated table and then clear staging table
        """
        if not run:
            self.logger.debug("Skip load job !")
            return "Success"

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using `{self.project_id}.{self.dataset_staging_id}.{self.table_name}` s
        on t.id = s.id
        when matched then
        update set
            t.full_name = s.full_name,
            t.email = s.email,
            t.phone = s.phone,
            t.date_of_birth = s.date_of_birth,
            t.school = s.school,
            t.major = s.major,
            t.company_type = s.company_type,
            t.position = s.position,
            t.level = s.level,
            t.study_area = s.study_area,
            t.area = s.area,
            t.lifecycle_stage = s.lifecycle_stage,
            t.updated_datetime = s.updated_datetime
        when not matched then
        insert (
            id,
            hubspot_link,
            full_name,
            email,
            phone,
            date_of_birth,
            school,
            major,
            company_type,
            position,
            level,
            study_area,
            area,
            lifecycle_stage,
            created_datetime,
            updated_datetime
            )
        values (
            id,
            hubspot_link,
            full_name,
            email,
            phone,
            date_of_birth,
            school,
            major,
            company_type,
            position,
            level,
            study_area,
            area,
            lifecycle_stage,
            created_datetime,
            updated_datetime
        )
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        self.logger.debug("Truncate staging table...")
        self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")

        return "Success"
