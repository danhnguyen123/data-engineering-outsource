from datetime import timedelta, datetime
import html as html_lib
import re
import pytz
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from plugins.config import config

from plugins.helper.logging_helper import LoggingHelper
from plugins.helper.airflow_helper import on_failure_callback_discord
from plugins.helper.discord_helper import send_discord_message
from plugins.helper.evn_helper import EVNHelper

NAMESPACE = 'power_outage'

# Customers checked every day (ChucNang=MaKhachHang). Add more entries to generate more tasks.
CUSTOMERS = [
    {"maKH": "PB02050009826", "label": "Nguyễn Anh Dũng - Nhà"},
    {"maKH": "PB02050058714", "label": "Nguyễn Văn Dũng - Trại heo"},
]

DEN_NGAY_DELTA_DAYS = 7  # denNgay = today + 7 days
NO_OUTAGE_MARKER = "không có lịch ngừng giảm"

local_tz = pendulum.timezone(config.DWH_TIMEZONE)
logger = LoggingHelper.get_configured_logger(NAMESPACE)
evn = EVNHelper(logger=logger)

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback_discord,
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
}


def _conf_or_param(context, key, default=None):
    conf = context['dag_run'].conf or {}
    if conf.get(key) is not None:
        return conf.get(key)
    params = context.get('params') or {}
    if params.get(key) is not None:
        return params.get(key)
    return default


def _default_dates():
    today = datetime.now(pytz.timezone(config.DWH_TIMEZONE)).date()
    tu_ngay = today.strftime("%d-%m-%Y")
    den_ngay = (today + timedelta(days=DEN_NGAY_DELTA_DAYS)).strftime("%d-%m-%Y")
    return tu_ngay, den_ngay


def _has_outage(raw_html: str) -> bool:
    # No-outage response contains "không có lịch ngừng giảm cung cấp điện"
    normalized = re.sub(r'\s+', ' ', raw_html.lower())
    return NO_OUTAGE_MARKER not in normalized


def _html_to_text(raw_html: str) -> str:
    # Convert the (entity-encoded) HTML fragment into readable plain text
    text = re.sub(r'(?i)<br\s*/?>', '\n', raw_html)
    text = re.sub(r'(?i)</(p|div|span)>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_lib.unescape(text)
    lines = [ln.strip() for ln in text.splitlines()]
    return '\n'.join(ln for ln in lines if ln)


def check_power_outage(code, label, chuc_nang, **context):
    """Query the outage schedule for one code; alert Discord if there is an outage, else log."""
    tu_ngay = _conf_or_param(context, 'tuNgay') or _default_dates()[0]
    den_ngay = _conf_or_param(context, 'denNgay') or _default_dates()[1]

    logger.info(f"[{label}] Tra cứu lịch cúp điện: ChucNang={chuc_nang}, code={code}, {tu_ngay} → {den_ngay}")
    raw_html = evn.get_outage_html(chuc_nang=chuc_nang, code=code, tu_ngay=tu_ngay, den_ngay=den_ngay)

    if not _has_outage(raw_html):
        logger.info(f"[{label}] Không có lịch ngừng giảm cung cấp điện ({tu_ngay} → {den_ngay}). Task chạy OK.")
        return "Success"

    body = _html_to_text(raw_html)
    message = f"⚡ [CẢNH BÁO CÚP ĐIỆN] {label} ({tu_ngay} → {den_ngay})\n{body}"
    if len(message) > 1900:
        message = message[:1900] + "\n... (đã cắt bớt)"

    send_discord_message(config.DISCORD_WEBHOOK, message)
    logger.info(f"[{label}] Có lịch cúp điện → đã gửi Discord.")
    return "Success"


def manual_lookup(**context):
    """Manual trigger: look up an arbitrary code with a chosen ChucNang (MaKhachHang or MaDonVi)."""
    code = _conf_or_param(context, 'ma')
    if not code:
        raise AirflowSkipException("Manual lookup: không có 'ma' trong conf → bỏ qua (chỉ chạy khi manual trigger).")

    chuc_nang = _conf_or_param(context, 'ChucNang', 'MaKhachHang')
    label = f"[Manual] {chuc_nang}: {code}"
    return check_power_outage(code=code, label=label, chuc_nang=chuc_nang, **context)


def contruct_params():
    return {
        "ChucNang": "MaKhachHang",  # manual trigger: "MaKhachHang" hoặc "MaDonVi"
        "ma": None,                 # manual trigger: mã maKH hoặc madvi cần tra cứu
        "tuNgay": None,             # optional override, format "DD-MM-YYYY"
        "denNgay": None,            # optional override, format "DD-MM-YYYY"
    }


with DAG(
    'dag-daily-power-outage',
    default_args=default_args,
    description='Cảnh báo lịch ngừng giảm cung cấp điện (EVN SPC) qua Discord',
    schedule_interval='0 7 * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=['daily', 'evn', 'power-outage', 'discord'],
    params=contruct_params(),
    dag_display_name="Cảnh báo cúp điện EVN hằng ngày 7AM"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    tasks = []

    # Daily per-customer checks (ChucNang=MaKhachHang), run in parallel
    for customer in CUSTOMERS:
        tasks.append(
            PythonOperator(
                task_id=f"check_{customer['maKH']}",
                python_callable=check_power_outage,
                op_kwargs={
                    "code": customer["maKH"],
                    "label": customer["label"],
                    "chuc_nang": "MaKhachHang",
                },
            )
        )

    # Manual ad-hoc lookup (skips on scheduled runs when no 'ma' is provided)
    tasks.append(
        PythonOperator(
            task_id="manual_lookup",
            python_callable=manual_lookup,
        )
    )

    start >> tasks >> end
