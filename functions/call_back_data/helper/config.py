"""
File: config.py
Purpose: This module is used to centralize environment variables and secrets
"""

import os

class Config():
    """
    Access environment variables here.
    """

    def __init__(self):
        """
        Load secret to config
        """

    LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')

config = Config()
