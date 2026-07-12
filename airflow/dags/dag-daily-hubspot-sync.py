from datetime import timedelta
from datetime import datetime, timezone
import subprocess
import yaml
import pendulum
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.config import config

from plugins.helper.logging_helper import LoggingHelper
from plugins.helper.redis_helper import RedisHelper
from plugins.helper.airflow_helper import on_failure_callback_discord
from plugins.helper.hubspot_helper import HubspotHelper
from plugins.helper.gcp_helper import BQHelper

from plugins.modules import (
    HubspotContactsETL,
    # HubspotDealsETL,
    # HubspotTicketsETL,
    # HubspotFeedbacksETL,
)

HUBSPOT_NAMESPACE = 'hubspot'
CONTACTS = 'contacts'
# DEALS = 'deals'
# TICKETS = 'tickets'
# FEEDBACK_SUBMISSIONS = 'feedback_submissions'

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(HUBSPOT_NAMESPACE)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)
hub_spot = HubspotHelper(logger=logger, redis=redis)
bq = BQHelper(logger=logger)

mapping_etl = {
    CONTACTS: HubspotContactsETL,
    # DEALS: HubspotDealsETL,
    # TICKETS: HubspotTicketsETL,
    # FEEDBACK_SUBMISSIONS: HubspotFeedbacksETL,
}

mapping_client = {
    HUBSPOT_NAMESPACE: hub_spot,
}

hubspot_tables_list = [CONTACTS]  # DEALS, TICKETS, FEEDBACK_SUBMISSIONS

CHECK_HUBSPOT_TOKEN = 'check_hubspot_token'
REFRESH_HUBSPOT_TOKEN = 'refresh_hubspot_token'
TOKEN_CACHED = 'token_cached'
TOKEN_READY = 'token_ready'


def check_hubspot_token(**kwargs):
    """Branch: use cached token if present in Redis, otherwise refresh via the CLI."""
    if redis.get_cached_value_for_key(config.HUBSPOT_ACCESS_TOKEN_REDIS):
        return TOKEN_CACHED
    return REFRESH_HUBSPOT_TOKEN


def refresh_hubspot_token(**kwargs):
    """
    Exchange the personal access key for a short-lived access token via the HubSpot CLI,
    read it from the CLI config file and cache it straight into Redis with a TTL derived
    from expiresAt. The token is never printed to stdout / pushed to XCom.
    """
    # --account is required so the CLI doesn't prompt interactively for an account name
    result = subprocess.run(
        ["hs", "account", "auth",
         "--account", config.HUBSPOT_CLI_ACCOUNT,
         "--personal-access-key", config.HUBSPOT_PERSONAL_ACCESS_KEY],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # surface the real CLI error (never contains the key itself, only messages)
        raise RuntimeError(
            f"`hs account auth` failed (exit {result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    with open(config.HUBSPOT_CLI_CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    accounts = {a.get('accountId'): a for a in cfg.get('accounts', [])}
    acc = accounts.get(cfg.get('defaultAccount')) or cfg['accounts'][0]
    token_info = acc['auth']['tokenInfo']

    access_token = token_info['accessToken']
    # Log a masked token + expiry for debugging without leaking the full secret
    masked = f"{access_token[:6]}...{access_token[-4:]}" if len(access_token) > 12 else "***"
    logger.info(f"HubSpot access token: {masked} | expiresAt: {token_info['expiresAt']}")

    # expiresAt like '2026-07-11T15:38:06.468Z' (UTC)
    expires_at = datetime.strptime(token_info['expiresAt'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    ttl = int((expires_at - datetime.now(timezone.utc)).total_seconds()) - config.HUBSPOT_TOKEN_TTL_MARGIN
    if ttl <= 0:
        raise ValueError(f"HubSpot access token already expired or TTL too small: expiresAt={token_info['expiresAt']}")

    redis.put_cached_value_for_key(config.HUBSPOT_ACCESS_TOKEN_REDIS, access_token, expire_time=ttl)
    logger.info(f"Cached HubSpot access token in Redis with TTL {ttl}s")

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback_discord,
    'trigger_rule' : 'all_done', #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
}

def call_python_etl(namespace, table_name, task_name, **kwargs):

    table_config = kwargs['dag_run'].conf.get(table_name) if kwargs['dag_run'].conf.get(table_name) else kwargs['params'].get(table_name)

    run = table_config.get(task_name)

    table_etl = mapping_etl.get(table_name)(
        logger=logger,
        project_id=config.PROJECT_ID,
        dataset_id=config.DATASET_ID,
        table_name=table_name,
        api_client=mapping_client.get(namespace),
        bq=bq,
        namespace=namespace,
        kwargs=kwargs
    )

    return getattr(table_etl, task_name)(run=run)


def contruct_params():
    params={
        "date_property": "lastmodifieddate",  # override to "createdate" when manually backfilling
        "start_date": None,  # manual backfill: "YYYY-MM-DD" (start of day)
        "end_date": None     # manual backfill: "YYYY-MM-DD" (end of day)
    }
    for table in hubspot_tables_list:
        params.update({
            table: {
                "extract": True,
                "load": True
            }
        })
    return params

with DAG(
    'dag-daily-hubspot-sync',
    default_args=default_args,
    description='ETL Data from HubSpot to BigQuery',
    schedule_interval='0 7 * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=['daily','elt','hubspot', 'bigquery'],
    params=contruct_params(),
    dag_display_name="ETL data from HubSpot daily at 7AM"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    check_token = BranchPythonOperator(
        task_id=CHECK_HUBSPOT_TOKEN,
        python_callable=check_hubspot_token,
    )

    refresh_token = PythonOperator(
        task_id=REFRESH_HUBSPOT_TOKEN,
        python_callable=refresh_hubspot_token,
    )

    token_cached = EmptyOperator(task_id=TOKEN_CACHED)

    # Runs whichever branch was taken succeeded (skipped branch is fine, a failed refresh blocks ETL)
    token_ready = EmptyOperator(task_id=TOKEN_READY, trigger_rule='none_failed_min_one_success')

    with TaskGroup(HUBSPOT_NAMESPACE, tooltip=f"Update all thing for {HUBSPOT_NAMESPACE}") as hubspot_etl_group:

        for table in hubspot_tables_list:

            with TaskGroup(table, tooltip=f"Update all thing for {table}") as table_group:

                extract = PythonOperator(
                    task_id=f"extract_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    trigger_rule='all_success',  # skip ETL when the token isn't ready
                    op_kwargs={
                        "namespace": HUBSPOT_NAMESPACE,
                        "table_name": table,
                        "task_name": "extract"
                        }
                    )

                load = PythonOperator(
                    task_id= f"load_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    trigger_rule='all_success',
                    op_kwargs={
                        "namespace": HUBSPOT_NAMESPACE,
                        "table_name": table,
                        "task_name": "load"
                        }
                    )

                extract >> load

    start >> check_token
    check_token >> refresh_token >> token_ready
    check_token >> token_cached >> token_ready
    token_ready >> hubspot_etl_group >> end
