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
from plugins.helper.airflow_helper import on_failure_callback_discord
from plugins.helper.gcp_helper import GCSHelper, BQHelper
import plugins.helper.time_helper as TimeHelper  
import envs.env as env

from plugins.helper.pancake_helper import PancakeHelper
from plugins.modules.pancake.page_customer import PageCustomerETL
from plugins.modules.pancake.conversations import ConversationsETL
from plugins.modules.pancake.messages import MessagesETL

PANCAKE_NAMESPACE = 'pancake'
CUSTOMERS = 'customers'
CONVERSATIONS = 'conversations'
MESSAGES = 'messages'

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(__name__)
bq = BQHelper(logger=logger)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)

pancake = PancakeHelper(logger=logger)

mapping_etl = {
    CUSTOMERS: PageCustomerETL,
    CONVERSATIONS: ConversationsETL,
    MESSAGES: MessagesETL
}

UPSTREAM_TABLES_LIST = [CUSTOMERS, CONVERSATIONS]

DOWNSTREAM_TABLES_LIST = [MESSAGES]

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback_discord,
    # 'on_retry_callback': on_failure_callback_discord,
    'trigger_rule' : 'all_done', #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1 if config.ENV == "prod" else 1,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
}

def contruct_params():
    params={
        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
    }
    for table in UPSTREAM_TABLES_LIST:
        params.update({
            table: {
                "extract": True,
                "load": True
            }              
        })

    for table in DOWNSTREAM_TABLES_LIST:
        params.update({
            table: {
                "extract": True,
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
        pancake=pancake,
        bq=bq,
        redis=redis,
        namespace=namespace,
        vars=vars,
        context=kwargs
    )

    return getattr(table_etl, task_name)()

with DAG(
    'dag-daily-pancake-sync',
    default_args=default_args,
    description='ETL Data from Pancake to BigQuery',
    schedule_interval='0 1 * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=["daily", 'elt','pancake', 'bigquery'],
    params=contruct_params(),
    # dag_display_name="ETL Data from Pancake to BigQuery"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup(PANCAKE_NAMESPACE, tooltip=f"Update all thing for {PANCAKE_NAMESPACE}") as etl_group:

        with TaskGroup("Upstream", tooltip=f"Update all thing for {PANCAKE_NAMESPACE}") as upstream:

            for table in UPSTREAM_TABLES_LIST:

                with TaskGroup(table, tooltip=f"Update all thing for {table}") as task_group:

                    with TaskGroup(f"extract_{table}", tooltip=f"Update all thing for pages") as extract_page_group:

                        for page in env.pancake_page:

                            extract = PythonOperator(
                                task_id=f"extract_{table}_{page.get('index')}",
                                python_callable=call_python_etl,
                                provide_context=True,
                                op_kwargs={
                                    "namespace": PANCAKE_NAMESPACE,
                                    "table_name": table,
                                    "task_name": "extract",
                                    "vars": {
                                        "start_date": TimeHelper.get_start_delta_date_format(delta=7),
                                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                                        "page": page
                                    }
                                }
                                )
                    
                    load = PythonOperator(
                        task_id= f"load_{table}",
                        python_callable=call_python_etl,
                        provide_context=True,
                        op_kwargs={
                            "namespace": PANCAKE_NAMESPACE,
                            "table_name": table,
                            "task_name": "load",
                            "vars": {
                                "start_date": TimeHelper.get_start_delta_date_format(delta=7),
                                "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1)                      
                            }
                        }
                        )
                
                    extract_page_group >> load

        with TaskGroup("Downstream", tooltip=f"Update all thing for {PANCAKE_NAMESPACE}") as downstream:

            for table in DOWNSTREAM_TABLES_LIST:

                with TaskGroup(table, tooltip=f"Update all thing for {table}") as task_group:

                    with TaskGroup(f"extract_{table}", tooltip=f"Update all thing for pages") as extract_page_group:

                        for page in env.pancake_page:

                            extract = PythonOperator(
                                task_id=f"extract_{table}_{page.get('index')}",
                                python_callable=call_python_etl,
                                provide_context=True,
                                op_kwargs={
                                    "namespace": PANCAKE_NAMESPACE,
                                    "table_name": table,
                                    "task_name": "extract",
                                    "vars": {
                                        "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                                        "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),
                                        "page": page
                                    }
                                }
                                )
                    
                    load = PythonOperator(
                        task_id= f"load_{table}",
                        python_callable=call_python_etl,
                        provide_context=True,
                        op_kwargs={
                            "namespace": PANCAKE_NAMESPACE,
                            "table_name": table,
                            "task_name": "load",
                            "vars": {
                                "start_date": TimeHelper.get_start_delta_date_format(delta=1),
                                "end_date": TimeHelper.get_end_delta_date_format(delta=1, step=1),                            
                            }
                        }
                        )
                
                    extract_page_group >> load      

        upstream >> downstream  

    start >> etl_group >> end

            
