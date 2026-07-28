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

# Units checked every day (ChucNang=MaDonVi). Only alert Discord when `filter_keyword`
# appears in the outage content (scheduled runs only; manual trigger has no filter).
DONVI = [
    {"madvi": "PB0204", "label": "Đơn vị PB0204", "filter_keyword": "Thôn Láng Gón 2"},
]

DEN_NGAY_DELTA_DAYS = 7  # denNgay = today + 7 days
NO_OUTAGE_MARKER = "không có lịch ngừng giảm"

# Discord message style: True = bullet-style formatted, False = raw decoded text.
# Can be overridden per run via conf/param "format_message".
FORMAT_MESSAGE = False

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


def _clean_html(fragment: str) -> str:
    fragment = re.sub(r'<[^>]+>', ' ', fragment)
    fragment = html_lib.unescape(fragment)
    return re.sub(r'\s+', ' ', fragment).strip()


def _parse_outage(raw_html: str):
    """Extract {unit, entries:[{where, time, cause}]} from the outage HTML fragment."""
    unit_m = re.search(r'Đơn vị:\s*</b>(.*?)</span>', raw_html, re.I | re.S)
    unit = _clean_html(unit_m.group(1)) if unit_m else None

    entries = []
    for chunk in re.split(r'<div\s+class="entry"', raw_html, flags=re.I)[1:]:
        where_m = re.search(r'KHU VỰC:\s*</b>(.*?)</span>', chunk, re.I | re.S)
        time_m = re.search(r'THỜI GIAN:\s*</b>(.*?)</span>', chunk, re.I | re.S)
        cause_m = re.search(r'ĐIỆN:\s*</b>(.*?)</span>', chunk, re.I | re.S)
        entry = {
            "where": _clean_html(where_m.group(1)) if where_m else None,
            "time": _clean_html(time_m.group(1)) if time_m else None,
            "cause": _clean_html(cause_m.group(1)) if cause_m else None,
        }
        if any(entry.values()):
            entries.append(entry)
    return unit, entries


def _format_outage_message(label, tu_ngay, den_ngay, raw_html):
    """Build a readable, bullet-style Discord message from the outage HTML."""
    unit, entries = _parse_outage(raw_html)

    lines = [
        f"⚡ **CẢNH BÁO CÚP ĐIỆN** — {label}",
        f"🗓️ Khoảng tra cứu: {tu_ngay} → {den_ngay}",
    ]
    if unit:
        lines.append(f"🏢 Đơn vị: {unit}")

    if entries:
        for i, e in enumerate(entries, 1):
            lines.append("")
            lines.append(f"**{i}. 📍 {e['where'] or 'N/A'}**")
            if e["time"]:
                lines.append(f"🕐 {e['time']}")
            if e["cause"]:
                lines.append(f"⚠️ Lý do: {e['cause']}")
    else:
        # Structure not recognized -> fall back to plain readable text
        lines.append("")
        lines.append(_html_to_text(raw_html))

    message = "\n".join(lines).strip()
    if len(message) > 1900:
        message = message[:1900] + "\n… (đã cắt bớt)"
    return message


def check_power_outage(code, label, chuc_nang, filter_keyword=None, **context):
    """Query the outage schedule for one code; alert Discord if there is an outage, else log.

    filter_keyword: if set, only send Discord when the keyword appears in the outage content.
    """
    tu_ngay = _conf_or_param(context, 'tuNgay') or _default_dates()[0]
    den_ngay = _conf_or_param(context, 'denNgay') or _default_dates()[1]

    logger.info(f"[{label}] Tra cứu lịch cúp điện: ChucNang={chuc_nang}, code={code}, {tu_ngay} → {den_ngay}")
    raw_html = evn.get_outage_html(chuc_nang=chuc_nang, code=code, tu_ngay=tu_ngay, den_ngay=den_ngay)

    if not _has_outage(raw_html):
        logger.info(f"[{label}] Không có lịch ngừng giảm cung cấp điện ({tu_ngay} → {den_ngay}). Task chạy OK.")
        return "Success"

    if filter_keyword:
        content_text = re.sub(r'\s+', ' ', _html_to_text(raw_html).lower())
        if filter_keyword.lower() not in content_text:
            logger.info(f"[{label}] Có lịch cúp điện nhưng KHÔNG có '{filter_keyword}' → bỏ qua, không gửi Discord.")
            return "Success"
        logger.info(f"[{label}] Có '{filter_keyword}' trong lịch cúp điện → gửi Discord.")

    formatted = _conf_or_param(context, 'format_message', FORMAT_MESSAGE)
    if isinstance(formatted, str):
        formatted = formatted.strip().lower() in ('1', 'true', 'yes', 'y')

    if formatted:
        message = _format_outage_message(label, tu_ngay, den_ngay, raw_html)
    else:
        body = _html_to_text(raw_html)
        message = f"⚡ [CẢNH BÁO CÚP ĐIỆN] {label} ({tu_ngay} → {den_ngay})\n{body}"
        if len(message) > 1900:
            message = message[:1900] + "\n… (đã cắt bớt)"

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
        "format_message": FORMAT_MESSAGE,  # True = bullet-style, False = raw text
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

    # Daily per-unit checks (ChucNang=MaDonVi), only alert when filter_keyword matches
    for dv in DONVI:
        tasks.append(
            PythonOperator(
                task_id=f"check_donvi_{dv['madvi']}",
                python_callable=check_power_outage,
                op_kwargs={
                    "code": dv["madvi"],
                    "label": dv["label"],
                    "chuc_nang": "MaDonVi",
                    "filter_keyword": dv["filter_keyword"],
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
