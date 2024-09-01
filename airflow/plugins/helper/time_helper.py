from datetime import datetime, date, timezone, timedelta
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import pytz
from config import config

def get_now_iso_date():
    return datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def get_start_date_format(date: str = "2024-01-01"):
    """
    Converts a date string in format like "2024-06-02" in UTC+7 to start date iso date
    """    
    # Get current datetime in UTC+7
    date_time = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone(offset=timedelta(hours=7)))

    # Convert to UTC and format as requested
    date_time_utc = date_time.astimezone(pytz.utc)

    # Get start of current date (set hour, minute, second, microsecond to 0)
    start_of_date = date_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    return start_of_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def get_end_date_format(date: str = "2024-01-01"):
    """
    Converts a date string in format like "2024-06-02" in UTC+7 to end date iso date
    """    
    # Get current datetime in UTC+7
    date_time = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone(offset=timedelta(hours=7)))

    # Convert to UTC and format as requested
    date_time_utc = date_time.astimezone(pytz.utc)

    start_of_date = date_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    # Get end of current date
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)

    return end_of_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def get_start_yesterday_date_format():
    now_utc = datetime.now(timezone.utc)
    timezone_offset = timezone(timedelta(hours=7))
    now_local = now_utc.astimezone(timezone_offset)
    yesterday = now_local - timedelta(days=1)
    start_of_yesterday_day = yesterday + timedelta(hours=0)
    return start_of_yesterday_day.strftime("%Y-%m-%d")

def get_end_yesterday_date_format():
    now_utc = datetime.now(timezone.utc)
    timezone_offset = timezone(timedelta(hours=7))
    now_local = now_utc.astimezone(timezone_offset)
    yesterday = now_local - timedelta(days=1)
    start_of_yesterday_day = yesterday + timedelta(hours=0)
    end_of_yesterday_day = start_of_yesterday_day + timedelta(days=1) - timedelta(microseconds=1)
    return end_of_yesterday_day.strftime("%Y-%m-%d")

def get_start_current_date_format():
    now_utc = datetime.now(timezone.utc)
    timezone_offset = timezone(timedelta(hours=7))
    now_local = now_utc.astimezone(timezone_offset)
    start_of_date = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_of_date.strftime("%Y-%m-%d")

def get_end_current_date_format():
    now_utc = datetime.now(timezone.utc)
    timezone_offset = timezone(timedelta(hours=7))
    now_local = now_utc.astimezone(timezone_offset)
    start_of_date = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)
    return end_of_date.strftime("%Y-%m-%d")