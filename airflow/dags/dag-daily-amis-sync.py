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
from plugins.helper.airflow_helper import on_failure_callback
from plugins.helper.gcp_helper import GCSHelper, BQHelper
import plugins.helper.time_helper as TimeHelper  

from plugins.helper.amis_web_helper import AmisWebHelper
from plugins.modules import (
    SupplyGoodsETL,
    StocksETL,
)

AMIS_NAMESPACE = 'amis'
SUPPLY_GOODS = 'supply_goods'
STOCKS = 'stocks'

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(__name__)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)
gcs = GCSHelper(logger=logger, bucket_name=config.BUCKET_NAME)
bq = BQHelper(logger=logger)
mongodb = MongoDBHeler(logger=logger)

amis = AmisWebHelper(logger=logger, redis=redis, mongodb=mongodb)

mapping_etl = {
    SUPPLY_GOODS: SupplyGoodsETL,
    STOCKS: StocksETL,
}

mapping_client = {
    AMIS_NAMESPACE: amis
}

amis_tables_list = [SUPPLY_GOODS, STOCKS]

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback,
    'trigger_rule' : 'all_done', #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3 if config.ENV == "prod" else 1,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
}

def contruct_params():
    params={}
    for table in amis_tables_list:
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
    'dag-daily-amis-sync',
    default_args=default_args,
    description='ETL Data from Misa Amis to BigQuery',
    schedule_interval='0 1 * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=["daily", 'elt','amis', 'bigquery'],
    params=contruct_params(),
    dag_display_name="ETL data from Misa Amis dailly"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup(AMIS_NAMESPACE, tooltip=f"Update all thing for {AMIS_NAMESPACE}") as amis_etl_group:

        for table in amis_tables_list:

            with TaskGroup(table, tooltip=f"Update all thing for {table}") as invoices_group:

                extract = PythonOperator(
                    task_id=f"extract_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    op_kwargs={
                        "namespace": AMIS_NAMESPACE,
                        "table_name": table,
                        "task_name": "extract",
                        "vars": {}
                    }
                    )
                
                transform = PythonOperator(
                    task_id= f"transform_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    op_kwargs={
                        "namespace": AMIS_NAMESPACE,
                        "table_name": table,
                        "task_name": "transform",
                        "vars": {}
                    }
                    )

                load = PythonOperator(
                    task_id= f"load_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    op_kwargs={
                        "namespace": AMIS_NAMESPACE,
                        "table_name": table,
                        "task_name": "load",
                        "vars": {}
                    }
                    )
            
                extract >> transform >> load

        start >> amis_etl_group >> end

            
