from datetime import timedelta
from datetime import datetime
import pendulum
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from plugins.config import config

from plugins.helper.logging_helper import LoggingHelper
from plugins.helper.redis_helper import RedisHelper
from plugins.helper.mongodb_helper import MongoDBHeler
from plugins.helper.airflow_helper import (
    on_failure_callback,
    on_retry_callback,
    sla_miss_callback
)
from plugins.helper.gcp_helper import GCSHelper, BQHelper
import plugins.helper.time_helper as TimeHelper  

from plugins.helper.eshop_helper import EshopHelper
from plugins.modules import (
    InvoicesETL,
    InvoiceDetailsETL,
    InventoryItemsETL
)

ESHOP_NAMESPACE = 'eshop'
INVOICES = 'invoices'
INVOICE_DETAILS = 'invoice_details'
INVENTORY_ITEMS = 'inventory_items'

RETRY_BATCH = 10 if config.ENV == "prod" else 5
RETRY_DETAIL = 100 if config.ENV == "prod" else 50
RETRY_DETAIL_DEPLAY = timedelta(seconds=10)

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(__name__)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)
gcs = GCSHelper(logger=logger, bucket_name=config.BUCKET_NAME)
bq = BQHelper(logger=logger)
mongodb = MongoDBHeler(logger=logger)

eshop = EshopHelper(logger=logger, redis=redis)

mapping_etl = {
    INVOICES: InvoicesETL,
    INVOICE_DETAILS: InvoiceDetailsETL,
    INVENTORY_ITEMS: InventoryItemsETL
}

mapping_client = {
    ESHOP_NAMESPACE: eshop
}

eshop_tables_list = [INVOICES, INVOICE_DETAILS, INVENTORY_ITEMS]

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback,
    'on_retry_callback': on_retry_callback,
    'sla': pendulum.duration(minutes=2),
    'sla_miss_callback': sla_miss_callback,
    'trigger_rule' : 'all_done', #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2 if config.ENV == "prod" else 1,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
}

def contruct_params():
    params={
        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
    }
    for table in eshop_tables_list:
        params.update({
            table: {
                "extract": True,
                "transform": True,
                "load": True
            }              
        })
    return params

def call_python_etl(namespace, table_name, task_name, vars, **kwargs):

    table_etl = mapping_etl.get(table_name)(
        logger=logger,
        project_id=config.PROJECT_ID,
        dataset_id=namespace,
        table_name=table_name,
        api_client=mapping_client.get(namespace),
        gcs=gcs,
        bq=bq,
        redis=redis,
        mongodb=mongodb,
        namespace=namespace,
        vars=vars,
        context=kwargs
    )

    return getattr(table_etl, task_name)()

with DAG(
    'dag-daily-eshop-sync',
    default_args=default_args,
    description='ETL Data from Misa Eshop API to BigQuery',
    schedule_interval='*/5 * * * *' if config.ENV == "prod" else None, 
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=["daily", 'elt','eshop', 'bigquery'],
    params=contruct_params(),
    dag_display_name="ETL data from Misa Eshop every 5 minutes"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup(ESHOP_NAMESPACE, tooltip=f"Update all thing for {ESHOP_NAMESPACE}") as eshop_etl_group:

        with TaskGroup(INVOICES, tooltip=f"Update all thing for {INVOICES}") as invoices_group:

            extract = PythonOperator(
                task_id=f"extract_{INVOICES}",
                python_callable=call_python_etl,
                provide_context=True,
                retries=RETRY_BATCH,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVOICES,
                    "task_name": "extract",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                    }
                }
                )
            
            transform = PythonOperator(
                task_id= f"transform_{INVOICES}",
                python_callable=call_python_etl,
                provide_context=True,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVOICES,
                    "task_name": "transform",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                    }
                }
                )

            load = PythonOperator(
                task_id= f"load_{INVOICES}",
                python_callable=call_python_etl,
                provide_context=True,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVOICES,
                    "task_name": "load",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                    }
                }
                )
        
            extract >> transform >> load

            
        with TaskGroup(INVOICE_DETAILS, tooltip=f"Update all thing for {INVOICE_DETAILS}") as invoice_details_group:

            extract = PythonOperator(
                task_id=f"extract_{INVOICE_DETAILS}",
                python_callable=call_python_etl,
                provide_context=True,
                retries=RETRY_DETAIL,
                retry_delay=RETRY_DETAIL_DEPLAY,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVOICE_DETAILS,
                    "task_name": "extract",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                    }
                }
                )
            
            transform = PythonOperator(
                task_id= f"transform_{INVOICE_DETAILS}",
                python_callable=call_python_etl,
                provide_context=True,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVOICE_DETAILS,
                    "task_name": "transform",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                    }
                }
                )

            load = PythonOperator(
                task_id= f"load_{INVOICE_DETAILS}",
                python_callable=call_python_etl,
                provide_context=True,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVOICE_DETAILS,
                    "task_name": "load",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                    }
                }
                )
        
            extract >> transform >> load

        with TaskGroup(INVENTORY_ITEMS, tooltip=f"Update all thing for {INVENTORY_ITEMS}") as inventory_items_group:

            extract = PythonOperator(
                task_id=f"extract_{INVENTORY_ITEMS}",
                python_callable=call_python_etl,
                provide_context=True,
                retries=RETRY_BATCH,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVENTORY_ITEMS,
                    "task_name": "extract",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                    }
                }
                )
            
            transform = PythonOperator(
                task_id= f"transform_{INVENTORY_ITEMS}",
                python_callable=call_python_etl,
                provide_context=True,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVENTORY_ITEMS,
                    "task_name": "transform",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                    }
                }
                )

            load = PythonOperator(
                task_id= f"load_{INVENTORY_ITEMS}",
                python_callable=call_python_etl,
                provide_context=True,
                op_kwargs={
                    "namespace": ESHOP_NAMESPACE,
                    "table_name": INVENTORY_ITEMS,
                    "task_name": "load",
                    "vars": {
                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                    }
                }
                )
        
            extract >> transform >> load
        
        [invoices_group >> invoice_details_group, inventory_items_group]

        start >> eshop_etl_group >> end

            
