from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
import pytz
from config import config

# Shared timezone objects (avoid recreating them on every call)
UTC7 = timezone(timedelta(hours=7))
tz = pytz.timezone("Asia/Bangkok")

DATE_FORMAT = "%Y-%m-%d"
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
DMY_FORMAT = "%d/%m/%Y %H:%M:%S"


def _now_utc7():
    """Current datetime in UTC+7."""
    return datetime.now(timezone.utc).astimezone(UTC7)


def _day_bounds(day):
    """Return (start, end) of the given UTC+7 day, end being 1 microsecond before midnight."""
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    return start, end


def _hour_bounds(moment):
    """Return (start, end) of the given hour, end being 1 microsecond before the next hour."""
    start = moment.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=1) - timedelta(microseconds=1)
    return start, end


def _to_epoch_ms(dt):
    return int(dt.timestamp() * 1000)


def get_now_iso_date():
    return datetime.now(pytz.utc).strftime(ISO_FORMAT)

def get_datetime_local():
    return datetime.now(tz).replace(microsecond=0, tzinfo=None)

def format_iso_date(date_time: datetime):
    """
    Converts a date string in format like "2024-06-02" at 00:00:00 in UTC+7 to iso date
    """
    return date_time.strftime(ISO_FORMAT)

def get_start_delta_date_format(delta=0):
    day = _now_utc7() - timedelta(days=delta)
    return day.strftime(DATE_FORMAT)

def get_end_delta_date_format(delta=0, step=0):
    start_of_delta_day = _now_utc7() - timedelta(days=delta)
    end_of_delta_day = start_of_delta_day + timedelta(days=step) - timedelta(microseconds=1)
    return end_of_delta_day.strftime(DATE_FORMAT)

def get_start_datetime_of_date(date):
    start_of_date = datetime.strptime(date, DATE_FORMAT).replace(hour=0, minute=0, second=0, microsecond=0)
    return tz.localize(start_of_date).astimezone(pytz.utc)

def get_end_datetime_of_date(date):
    start_of_date = datetime.strptime(date, DATE_FORMAT).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)
    return tz.localize(end_of_date).astimezone(pytz.utc)

def get_now_local_time():
    return datetime.now().astimezone(pytz.timezone(config.DWH_TIMEZONE))

def get_now_local_date():
    return get_now_local_time().strftime(DATE_FORMAT)

def get_unix_timestamp(datetime: datetime):
    return int(datetime.timestamp())

def utc7_str_to_unix(timestamp_str: str) -> int:
    """
    Convert datetime string (UTC+7) to Unix timestamp (seconds)

    Input format: YYYY-MM-DDTHH:MM:SS.microseconds
    Example: "2025-10-18T07:17:37.000000"
    """
    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=UTC7)
    return int(dt.timestamp())

def get_start_end_current_date_hour_epoch():
    """Start/end epoch (ms) of the current hour in UTC+7."""
    start_of_hour, end_of_hour = _hour_bounds(_now_utc7())
    return _to_epoch_ms(start_of_hour), _to_epoch_ms(end_of_hour)

def get_start_end_current_date_epoch(date: str = "2024-01-01"):
    """
    Converts a date string in format like "2024-06-02" in UTC+7 to start date epoch and end date epoch (ms)
    """
    start_of_date, end_of_date = _day_bounds(datetime.strptime(date, DATE_FORMAT).replace(tzinfo=UTC7))
    return _to_epoch_ms(start_of_date), _to_epoch_ms(end_of_date)

def get_start_end_current_date_hour_format():
    """Start/end of the current hour in UTC+7 as "%d/%m/%Y %H:%M:%S" strings."""
    start_of_hour, end_of_hour = _hour_bounds(_now_utc7())
    return start_of_hour.strftime(DMY_FORMAT), end_of_hour.strftime(DMY_FORMAT)

def get_start_end_current_date_format(date: str = "2024-01-01"):
    """
    Converts a date string in format like "2024-06-02" in UTC+7 to start/end date as "%d/%m/%Y %H:%M:%S" strings
    """
    start_of_date, end_of_date = _day_bounds(datetime.strptime(date, DATE_FORMAT).replace(tzinfo=UTC7))
    return start_of_date.strftime(DMY_FORMAT), end_of_date.strftime(DMY_FORMAT)

def get_format_date_hour_current():
    return _now_utc7().strftime("%Y/%m/%d/%H")

def convert_to_local_datetime_string(datetime_str):
    """
    Converts a date string in format like "2024-06-02T10:06:55.179Z" to in UTC+7 like "2024-06-02T17:06:55.179000"
    """
    dt_local = parse(datetime_str).astimezone(pytz.timezone(config.DWH_TIMEZONE))
    return dt_local.strftime("%Y-%m-%dT%H:%M:%S.%f")

def convert_formated_to_local_datetime_string(datetime_str):
    """
    Converts a date string in format like "18/06/2024 18:50:29" in UTC+7 to datetime string like "2024-06-02T17:06:55.179000"
    """
    dt = datetime.strptime(datetime_str, DMY_FORMAT).replace(tzinfo=UTC7)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
