"""
File: jsonlogging.py
Purpose: Define logger for logging
"""

import logging
from helper.config import config

def get_configured_logger(logger_name: str):
    """
    Return a JSON logger to use

    :param logger_name: logger name
    :return: the logger
    """
    log_level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
        "fatal": logging.FATAL,
    }

    _handler = logging.StreamHandler()
    _logger = logging.getLogger(logger_name)
    _logger.addHandler(_handler)
    _logger.setLevel(log_level_map[config.LOG_LEVEL.lower()])
    return _logger


