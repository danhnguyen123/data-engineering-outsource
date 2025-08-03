from datetime import datetime, date, timezone, timedelta
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import pytz
from config import config

tz = pytz.timezone("Asia/Bangkok")

def get_now_iso_date():
    return datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def format_iso_date(date_time: datetime):
    """
    Converts a date string in format like "2024-06-02" at 00:00:00 in UTC+7 to iso date
    """    
    return date_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def get_start_delta_date_format(delta=0):
    now_utc = datetime.now(timezone.utc)
    timezone_offset = timezone(timedelta(hours=7))
    now_local = now_utc.astimezone(timezone_offset)
    delta = now_local - timedelta(days=delta)
    start_of_delta_day = delta + timedelta(hours=0)
    return start_of_delta_day.strftime("%Y-%m-%d")

def get_end_delta_date_format(delta=0, step=0):
    now_utc = datetime.now(timezone.utc)
    timezone_offset = timezone(timedelta(hours=7))
    now_local = now_utc.astimezone(timezone_offset)
    delta = now_local - timedelta(days=delta)
    start_of_delta_day = delta + timedelta(hours=0)
    end_of_delta_day = start_of_delta_day + timedelta(days=step) - timedelta(microseconds=1)
    return end_of_delta_day.strftime("%Y-%m-%d")

def get_start_datetime_of_date(date):
    date_time = datetime.strptime(date, "%Y-%m-%d")
    start_of_date = date_time.replace(hour=0, minute=0, second=0, microsecond=0)
    dt_local = tz.localize(start_of_date)
    dt_utc = dt_local.astimezone(pytz.utc)
    return dt_utc

def get_end_datetime_of_date(date):
    date_time = datetime.strptime(date, "%Y-%m-%d")
    start_of_date = date_time.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)
    dt_local = tz.localize(end_of_date)
    dt_utc = dt_local.astimezone(pytz.utc)
    return dt_utc

def get_unix_timestamp(datetime: datetime):
    return int(datetime.timestamp())

