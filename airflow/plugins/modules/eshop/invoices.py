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

class InvoicesETL:
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
            vars: Dict,
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
        self.vars = vars
        self.context = context

        self.run_config = self.context['dag_run'].conf.get(table_name) if self.context['dag_run'].conf.get(table_name) else self.context['params'].get(table_name)
        self.start_date = self.context['dag_run'].conf.get('start_date') if self.context['dag_run'].conf.get('start_date') else self.vars["start_date"] 
        self.end_date = self.context['dag_run'].conf.get('end_date') if self.context['dag_run'].conf.get('end_date') else self.vars["end_date"] 
        self.dataset_staging_id = config.DATASET_STAGING_ID
        self.start_datetime = TimeHelper.format_iso_date(self.start_date)
        self.end_datetime = TimeHelper.format_iso_date(self.end_date)

    def extract(self):
        """
        Batch process data from Eshop and upload to GCS
        """
        if not self.run_config.get("extract"):
            self.logger.debug("Skip extract job !")
            return "Success"

        self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")

        self.logger.debug(f"Get last page")
        page =  self.redis.get_cached_value_for_key(config.ESHOP_INVOICE_LATEST_PAGE_REDIS)
        page = int(page) if page else 1

        self.logger.debug(f"Get data {self.table_name} from Eshop | page {page}")

        results = self.eshop.get_invoices(page=page, from_datetime=self.start_datetime, to_datetime=self.end_datetime)
    
        if results:
            self.context['ti'].xcom_push(key=config.NEW_DATA, value=True)
        else:
            self.context['ti'].xcom_push(key=config.NEW_DATA, value=False)
            self.logger.debug("There is no new data. Skip extract job !")
            return "Success"
        
        # data_invoices = []
        # list_invoices = []
        for invoice in results:
            # data_invoices.append({**invoice})
            # list_invoices.append({"_id": invoice.get("InvoiceId"), "InvoiceId": invoice.get("InvoiceId"), "GetDetailStatus": False})
            self.mongodb.update_one(database=config.MONGODB_CACHING, collection=self.table_name, 
                                    contition={"_id": invoice.get("InvoiceId")}, 
                                    update_query={"$set": {"InvoiceId": invoice.get("InvoiceId"), "GetDetailStatus": False}},
                                    upsert=True
                                    )

            self.mongodb.update_one(database=config.MONGODB_STAGING, collection=self.table_name, 
                                    contition={"_id": invoice.get("InvoiceId")}, 
                                    update_query={"$set": invoice},
                                    upsert=True
                                    )
        # self.mongodb.insert_many(database=config.MONGODB_CACHING, collection=self.table_name, list_document=list_invoices)

        # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)
        # file_name = f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.start_date}/{self.end_date}/{config.PREFIX_JSON_FILE}_{page}.json"
        # self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
        # self.gcs.upload_json(json_string=json_data, file_name=file_name)

        self.redis.put_cached_value_for_key(config.ESHOP_INVOICE_LATEST_PAGE_REDIS, page, 300)

        while len(results) == config.ESHOP_PAGE_LIMIT:
            page += 1
            self.logger.debug(f"Get data {self.table_name} from Eshop | page {page}")

            results = self.eshop.get_invoices(page=page, from_datetime=self.start_datetime, to_datetime=self.end_datetime)

            if results:
                # data_invoices = []
                # list_invoices = []
                for invoice in results:
                    # data_invoices.append({**invoice})
                    # list_invoices.append({"_id": invoice.get("InvoiceId"), "InvoiceId": invoice.get("InvoiceId"), "GetDetailStatus": False})
                    self.mongodb.update_one(database=config.MONGODB_CACHING, collection=self.table_name, 
                                            contition={"_id": invoice.get("InvoiceId")}, 
                                            update_query={"$set": {"InvoiceId": invoice.get("InvoiceId"), "GetDetailStatus": False}},
                                            upsert=True
                                            )

                    self.mongodb.update_one(database=config.MONGODB_STAGING, collection=self.table_name, 
                                            contition={"_id": invoice.get("InvoiceId")}, 
                                            update_query={"$set": invoice},
                                            upsert=True
                                            )
                # self.mongodb.insert_many(database=config.MONGODB_CACHING, collection=self.table_name, list_document=list_invoices)

                # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)
                # file_name = f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.start_date}/{self.end_date}/{config.PREFIX_JSON_FILE}_{page}.json"
                # self.logger.debug(f"Upload json data {self.table_name} to GCS: {file_name}")
                # self.gcs.upload_json(json_string=json_data, file_name=file_name)

                self.redis.put_cached_value_for_key(config.ESHOP_INVOICE_LATEST_PAGE_REDIS, page, 300)

        self.redis.remove_cached_value_for_key(config.ESHOP_INVOICE_LATEST_PAGE_REDIS)

        return "Success"  

    def transform(self):
        """
        Pull data from GCS, transform data and upload to staging table
        """
        if not self.run_config.get("transform"):
            self.logger.debug("Skip tranform job !")
            return "Success"
        
        if self.context['ti'].xcom_pull(task_ids=f"{self.namespace}.{self.table_name}.extract_{self.table_name}", key=config.NEW_DATA):
            self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"            

        self.logger.debug("Truncate staging table...")
        truncate_result = self.bq.execute(f"truncate table `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`")
        self.logger.debug(f"Truncate staging table, result {truncate_result}")

        # blobs  = self.gcs.bucket.list_blobs(prefix=f"{config.PREFIX_ESHOP_BUCKET}/{self.table_name}/{self.start_date}/{self.end_date}/")
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

        df["InvoiceDate"] = df["InvoiceDate"].map(lambda i: i.split("+")[0])
        df = df.drop(columns=['InvoiceTime', 'Point'])

        df = df[[
            'InvoiceId',
            'InvoiceNumber',
            'InvoiceType',
            'InvoiceDate',
            'BranchId',
            'BranchName',
            'TotalAmount',
            'CostAmount',
            'TaxAmount',
            'TotalItemAmount',
            'VATAmount',
            'DiscountAmount',
            'CashAmount',
            'CardAmount',
            'VoucherAmount',
            'DebitAmount',
            'ActualAmount',
            'Cashier',
            'SaleStaff',
            'PaymentStatus',
            'IsCOD',
            'AdditionBillType',
            'SaleChannelName',
            'HasConnectedShippingPartner',
            'PartnerStatus',
        ]]

        self.logger.debug(f"The DataFrame has {len(df)} rows.")
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
            self.logger.debug(f"start - {self.start_datetime} | end - {self.end_datetime}")
        else:
            self.logger.debug("There is no new data. Skip transform job !")
            return "Success"   

        merge_query = f'''
        merge `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        using (
            select *
            from `{self.project_id}.{self.dataset_staging_id}.{self.table_name}`
            where 1=1
            qualify row_number() over(partition by InvoiceId order by InvoiceDate desc) = 1
        ) s
        on t.InvoiceId = s.InvoiceId
        when matched then
        update set 
            t.InvoiceNumber = s.InvoiceNumber,
            t.InvoiceType = s.InvoiceType,
            t.InvoiceDate = s.InvoiceDate,
            t.BranchId = s.BranchId,
            t.BranchName = s.BranchName,
            t.Barcode = s.Barcode,
            t.TotalAmount = s.TotalAmount,
            t.CostAmount = s.CostAmount,
            t.TaxAmount = s.TaxAmount,
            t.TotalItemAmount = s.TotalItemAmount,
            t.VATAmount = s.VATAmount,
            t.DiscountAmount = s.DiscountAmount,
            t.CashAmount = s.CashAmount,
            t.CardAmount = s.CardAmount,
            t.VoucherAmount = s.VoucherAmount,
            t.DebitAmount = s.DebitAmount,
            t.ActualAmount = s.ActualAmount,
            t.CustomerName = s.CustomerName,
            t.Cashier = s.Cashier,
            t.SaleStaff = s.SaleStaff,
            t.Note = s.Note,
            t.PaymentStatus = s.PaymentStatus,
            t.Tel = s.Tel,
            t.Address = s.Address,
            t.MemberLevelName = s.MemberLevelName,
            t.DeliveryDate = s.DeliveryDate,
            t.IsCOD = s.IsCOD,
            t.AdditionBillType = s.AdditionBillType,
            t.SaleChannelName = s.SaleChannelName,
            t.ReturnExchangeRefNo = s.ReturnExchangeRefNo,
            t.HasConnectedShippingPartner = s.HasConnectedShippingPartner,
            t.DeliveryCode = s.DeliveryCode,
            t.ShippingPartnerName = s.ShippingPartnerName,
            t.PartnerStatus = s.PartnerStatus
        when not matched then
        insert row
        '''
        self.logger.debug(merge_query)
        results = self.bq.execute(query=merge_query)
        self.logger.debug(results)

        return "Success"  