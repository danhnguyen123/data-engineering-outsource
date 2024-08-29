import requests
import json
import hmac
import hashlib
from requests.exceptions import HTTPError, RequestException
from urllib import request, parse
from config import config
from logging import Logger
from helper.redis_helper import RedisHelper, RedisError
import helper.time_helper as TimeHelper
import copy

ACCOUNT_OBJECT = "account_object"
INVENTORY_ITEM = "inventory_item"
STOCK = "stock"

DICTIONARY = {
    ACCOUNT_OBJECT: 1,
    INVENTORY_ITEM: 2,
    STOCK: 3
}

class AmisHelper:

    session = None

    def __init__(self, logger: Logger, redis: RedisHelper):
        self.logger = logger
        self.redis = redis
        self.session = self.request_session()
    
    @property
    def headers(self):
        _headers = {
            "Content-Type": "application/json",
            "X-MISA-AccessToken": self.access_token
        }

        return _headers

    @property
    def access_token(self):
        try:
            _cached_token = self.redis.get_cached_value_for_key(config.AMIS_ACCESS_TOKEN_REDIS)
            if _cached_token:
                self.logger.debug(f"Get access token from Redis cache: {_cached_token}")
                return _cached_token
            else:
                _access_token = self._get_access_token()
                self.logger.debug(f"Get access token from Amis: {_access_token}")
                return _access_token
            
        except RedisError as re:
            raise re
        except HTTPError as he:
            raise he
        except Exception as e:
            raise e

    def request_session(self):
        """
        Create a session with retry handling

        :param session (_type_, optional): user session.
        :returns session: request session
        """
        if self.session is None:
            session = requests.Session()
        return session

    def _get_access_token(self):
        """
        API Doc: https://actdocs.misa.vn/g2/graph/ACTOpenAPIHelp/index.html#2-1
        """
        url = f"{config.AMIS_URL}/api/oauth/actopen/connect"
        headers = {
            "Content-Type" : "application/json"
        }
        payload = {
            "app_id": config.AMIS_APP_ID,
            "access_code": config.AMIS_ACCESS_CODE,
            "org_company_code": config.AMIS_COMPANY_CODE
        }

        try:
            response = requests.post(url=url, headers=headers, json=payload, timeout=config.AMIS_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("Success"):
                data_raw = response_json.get("Data", {})
                data = json.loads(data_raw)

                access_token = data.get("access_token")
                
                self.redis.put_cached_value_for_key(config.AMIS_ACCESS_TOKEN_REDIS, access_token, config.AMIS_ACCESS_TOKEN_TTL)

                return access_token

            else:
                message = f"Error when getting access token from Amis API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as he:
            raise he       
        except RequestException as re:
            error_msg = f"Request exception when getting info from Amis API, error: {re}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def get_dictionary(self, page: int=1, limit: int=config.AMIS_PAGE_LIMIT, data_type: str=None, last_sync_time: str=None):
        """
        Get list dictionary from Amis.

        API Doc: https://actdocs.misa.vn/g2/graph/ACTOpenAPIHelp/index.html#2-4
        """
        url = f"{config.AMIS_URL}/apir/sync/actopen/get_dictionary"
        payload = json.dumps({
            "data_type": DICTIONARY[data_type],
            "branch_id": None,
            "skip": 0,
            "take": limit,
            "app_id": config.AMIS_APP_ID,
            "last_sync_time": last_sync_time #2021-12-25 14:15:02
        })

        try:
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.AMIS_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("Success"):
                data_raw = response_json.get("Data", {})
                data = json.loads(data_raw)
                return data

            else:
                message = f"Error when getting {DICTIONARY[data_type]} from Amis API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as he:
            raise he       
        except RequestException as re:
            error_msg = f"Request exception when getting {DICTIONARY[data_type]} from Amis API, error: {re}"
            raise Exception(error_msg)
        except Exception as e:
            raise e