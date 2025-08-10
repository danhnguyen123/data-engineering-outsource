import os

class AppConfig(object):
    """
    Access environment variables here.
    """
    def __init__(self):
        """
        Load secret to config
        """
    ENV = os.getenv("ENV","dev")
    SERVICE_NAME = 'myspa-etl'
    LOG_LEVEL = os.getenv("LOG_LEVEL", "debug")

    PROJECT_ID = "data-analytics-service"
    DATASET_ID = "myspa"
    DATASET_STAGING_ID = "staging"

    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "DISCORD_WEBHOOK")

config = AppConfig()
