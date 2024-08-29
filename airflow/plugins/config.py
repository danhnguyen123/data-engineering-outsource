import os
import dotenv
import yaml
dotenv.load_dotenv('/opt/airflow/secrets/.env')

class AppConfig(object):
    """
    Access environment variables here.
    """
    def __init__(self):
        """
        Load secret to config
        """
    ENV = os.getenv("ENV","dev")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "debug")

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = os.getenv("REDIS_PORT", 6379)

    PROJECT_ID = os.getenv("PROJECT_ID", "unknown")
    DATASET_ID = os.getenv("DATASET_ID", "unknown")
    DATASET_STAGING_ID = os.getenv("DATASET_STAGING_ID", "unknown")
    DATASET_WAREHOUSE_ID = os.getenv("DATASET_WAREHOUSE_ID", "unknown")
    SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT", "unknown")
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "unknown")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "unknown")
    PREFIX_ESHOP_BUCKET = "eshop"
    PREFIX_JSON_FILE = "json"

    HTTP_CODE_RETRY = [500, 502, 503, 504]

    # MISA Eshop
    ESHOP_DOMAIN = os.getenv("ESHOP_DOMAIN", "unknown")
    ESHOP_APP_ID = os.getenv("ESHOP_APP_ID", "unknown")
    ESHOP_SECRET_KEY = os.getenv("ESHOP_SECRET_KEY", "unknown")
    ESHOP_URL = os.getenv("ESHOP_URL", "unknown")
    ESHOP_ACCESS_TOKEN_REDIS = "ESHOP_ACCESS_TOKEN_REDIS"
    ESHOP_ACCESS_TOKEN_TTL = 43200 # 12 hours, Misa Eshop expire 24h
    ESHOP_COMPANY_CODE_REDIS = "ESHOP_COMPANY_CODE_REDIS"
    ESHOP_ENVIRONMENT_REDIS = "ESHOP_ENVIRONMENT_REDIS"
    ESHOP_PAGE_LIMIT = 100
    ESHOP_REQUEST_TIMEOUT = 30
    ESHOP_TOTAL_RETRY = 5
    ESHOP_BACKOFF_FACTOR = 0.1
    ESHOP_INVOICE_LATEST_PAGE_REDIS = "ESHOP_INVOICE_LATEST_PAGE_REDIS"
    ESHOP_INVENTORY_LATEST_PAGE_REDIS = "ESHOP_INVENTORY_LATEST_PAGE_REDIS"

    # MISA Amis
    AMIS_URL = os.getenv("AMIS_URL", "unknown")
    AMIS_APP_ID = os.getenv("AMIS_APP_ID", "unknown")
    AMIS_ACCESS_CODE = os.getenv("AMIS_ACCESS_CODE", "unknown")
    AMIS_COMPANY_CODE = os.getenv("AMIS_COMPANY_CODE", "unknown")
    AMIS_ACCESS_TOKEN_REDIS = "AMIS_ACCESS_TOKEN_REDIS"
    AMIS_ACCESS_TOKEN_TTL = 36000 # 10 hours, Misa Amis expire 12h
    AMIS_PAGE_LIMIT = 100
    AMIS_REQUEST_TIMEOUT = 30

    DWH_TIMEZONE = 'Asia/Ho_Chi_Minh'
    DWH_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    NEW_DATA = "has_new_data"

    LARK_ALERT_GROUP_ID = ""


    MONGODB_HOST = "mongodb"
    MONGODB_PORT = 27017
    MONGODB_USER = os.getenv("MONGODB_USER", "unknown")
    MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD", "unknown")
    MONGODB_CONN = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/"
    MONGODB_STAGING = "staging"
    MONGODB_TEMP = "temp"


config = AppConfig()
