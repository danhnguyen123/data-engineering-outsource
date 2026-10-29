from datetime import timedelta
from datetime import datetime
import time
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from plugins.config import config

from plugins.helper.logging_helper import LoggingHelper
from plugins.helper.redis_helper import RedisHelper
from plugins.helper.airflow_helper import on_failure_callback_discord
from plugins.helper.discord_helper import send_discord_message
from plugins.helper.accsmarket_helper import AccsMarketHelper
from plugins.modules.accsmarket.catalog import parse_catalog_items
from plugins.modules.accsmarket.sales_alert import evaluate_sales, build_alert_messages

NAMESPACE = 'accsmarket_sales_alert'

# Catalog pages to watch. Add entries to watch more catalogs (one task each, run in parallel).
CATALOGS = [
    {"key": "gmail_aged", "path": "/en/catalog/gmail/s-otlezhkoj-25"},
]

# "Good item" filter
MIN_ORDERS = 100              # orders badge 100+ / 1k+ / 10k+
MAX_REJECT_RATE_PCT = 2.0     # reject rate <= 2%
# Alert when at least MIN_SOLD pcs were sold within SALES_WINDOW_MINUTES
# (set the window to 15 to compare strictly with the previous run)
MIN_SOLD = 10
SALES_WINDOW_MINUTES = 60

REDIS_BASELINE_KEY = "ACCSMARKET_SALES_BASELINE"
REDIS_BASELINE_TTL = 7 * 24 * 3600  # seconds

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(NAMESPACE)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)
accsmarket = AccsMarketHelper(logger=logger)

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback_discord,
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # runs every 15 minutes; the helper already retries the request
    'catchup' : False,
}


def check_fast_selling(catalog, **context):
    raw_html = accsmarket.get_catalog_html(catalog["path"])
    items = parse_catalog_items(raw_html)
    now = int(time.time())

    redis_key = f"{REDIS_BASELINE_KEY}:{catalog['key']}"
    baseline = redis.get_cached_value_for_key_as_dict(redis_key) or {}

    alerts, new_baseline = evaluate_sales(
        items=items,
        baseline=baseline,
        now=now,
        min_sold=MIN_SOLD,
        window_minutes=SALES_WINDOW_MINUTES,
        min_orders=MIN_ORDERS,
        max_reject_rate_pct=MAX_REJECT_RATE_PCT,
    )
    if new_baseline:
        redis.put_cached_value_for_key_as_dict(redis_key, new_baseline, expire_time=REDIS_BASELINE_TTL)

    in_stock = sum(1 for i in items if i.get("qty"))
    if not baseline:
        logger.info(f"[{catalog['key']}] First run: saved baseline for {len(new_baseline)} items ({in_stock} in stock).")

    if not alerts:
        logger.info(f"[{catalog['key']}] {len(items)} items ({in_stock} in stock): no new/restocked good item "
                    f"and none sold >= {MIN_SOLD} within {SALES_WINDOW_MINUTES} min. Task chạy OK.")
        return "Success"

    for message in build_alert_messages(alerts, catalog["key"]):
        send_discord_message(config.DISCORD_WEBHOOK, message)
    logger.info(f"[{catalog['key']}] Sent Discord alert for {len(alerts)} good items: "
                f"{[(a['kind'], a['id'], a.get('sold')) for a in alerts]}")
    return "Success"


with DAG(
    'dag-accsmarket-sales-alert',
    default_args=default_args,
    description='Alert Discord about good AccsMarket items: newly found, back in stock or selling fast',
    schedule_interval='*/15 * * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,  # runs share the Redis baseline, never overlap them
    dagrun_timeout=timedelta(minutes=10),
    tags=['15mins', 'accsmarket', 'alert', 'discord'],
    dag_display_name="AccsMarket - alert hàng tốt (15 phút)"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    tasks = [
        PythonOperator(
            task_id=f"check_{catalog['key']}",
            python_callable=check_fast_selling,
            op_kwargs={"catalog": catalog},
        )
        for catalog in CATALOGS
    ]

    start >> tasks >> end
