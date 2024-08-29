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
from helper.mongodb_helper import MongoDBHeler
import copy
import yaml
import os
import dotenv
import envs.amis_web_config as awc


class AmisWebHelper:

    session = None

    def __init__(self, logger: Logger, redis: RedisHelper, mongodb: MongoDBHeler):
        self.logger = logger
        self.redis = redis
        self.mongodb = mongodb
        self.session = self.request_session()

    @property
    def headers(self):
        _access_token = self.access_token
        _data = self.mongodb.find(config.MONGODB_CACHING, config.AMIS_WEB_COLLECTION, {"_id": "login"}, {"_id": 0})
        _context = _data[0].get("Context")
        _context["Language"] = "vi"
        context = json.dumps(_context).replace(" ", "")

        _headers = {
            'Accept': awc.headers.get("Accept"),
            'Accept-Language': awc.headers.get("Accept-Language"),
            'Authorization': 'Bearer ' + _access_token,
            'Connection': awc.headers.get("Connection"),
            'Content-Type': 'application/json',
            'Cookie': awc.headers.get("Cookie"),
            'Origin': 'https://actapp.misa.vn',
            # 'Referer': 'https://actapp.misa.vn/app/DI/DIInventoryItems',
            'Sec-Fetch-Dest': awc.headers.get("Sec-Fetch-Dest"),
            'Sec-Fetch-Mode': awc.headers.get("Sec-Fetch-Mode"),
            'Sec-Fetch-Site': awc.headers.get("Sec-Fetch-Site"),
            'User-Agent': awc.headers.get("User-Agent"),
            'X-Device': awc.headers.get("X-Device"),
            'X-MISA-Context': context,
            'sec-ch-ua': awc.headers.get("sec-ch-ua"),
            'sec-ch-ua-mobile': awc.headers.get("sec-ch-ua-mobile"),
            'sec-ch-ua-platform': awc.headers.get("sec-ch-ua-platform"),
        }

        return _headers

    @property
    def access_token(self):
        try:
            _cached_token = self.redis.get_cached_value_for_key(config.AMIS_WEB_ACCESS_TOKEN_REDIS)
            if _cached_token:
                self.logger.debug(f"Get access token from Redis cache: ***")
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
        Get access token from Amis Web.
        """
        url = f"{config.AMIS_WEB_URL}/g2/api/auth/v1/account/login/misa_id"

        try:
            response = requests.post(url=url, headers=awc.headers, data=awc.payload, timeout=config.AMIS_WEB_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                data = response_json.get("Data", {})
                # data = json.loads(data_raw)

                if not data:
                    message = self.logger.error(f"Unknow error when getting access token from Amis Web API, {response.status_code}, {response.text}")
                    raise Exception(message)

                access_token = data.get("AccessToken")
                self.redis.put_cached_value_for_key(config.AMIS_WEB_ACCESS_TOKEN_REDIS, access_token.get("Token"), int(access_token.get("TokenExpired")))

                del data["AccessToken"]
                self.mongodb.update_one(database=config.MONGODB_CACHING, collection=config.AMIS_WEB_COLLECTION, 
                                        contition={"_id": "login"}, 
                                        update_query={"$set": data},
                                        upsert=True
                                        )

                return access_token.get("Token")

            else:
                message = f"Error when getting access token from Amis Web API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as he:
            raise he       
        except RequestException as re:
            error_msg = f"Request exception when getting info from Amis Web API, error: {re}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def get_inventory_items(self, page: int=1, limit: int=config.AMIS_WEB_PAGE_LIMIT, load_mode: int=3):
        """
        Get inventory items from Amis Web.
        """
        _headers = self.headers
        headers = copy.deepcopy(_headers)
        headers["Referer"] = 'https://actapp.misa.vn/app/DI/DIInventoryItems'

        url = f"{config.AMIS_WEB_URL}/g2/api/db/v1/list/get_data"

        payload = json.dumps({
            "stockItemState": -1,
            "isPostToManagementBook": 0,
            "isIncludeDependentBranch": True,
            "isFilter": False,
            "sort": "[{\"property\":\"inventory_item_code\",\"desc\":false}]",
            "pageIndex": page,
            "pageSize": limit,
            "useSp": False,
            "view": "view_di_inventory_item",
            "summaryColumns": ",closing_amount",
            "dataType": "di_inventory_item",
            "isGetTotal": True,
            "is_filter_branch": False,
            "current_branch": "ac18dd4c-5881-4d5e-ad4f-48d535c32477",
            "is_multi_branch": False,
            "is_dependent": True,
            "loadMode": load_mode # 2: List items, 3: Count items
        })

        try:
            response = self.session.post(url=url, headers=headers, data=payload, timeout=config.AMIS_WEB_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                data = response_json.get("Data", {})
                # data = json.loads(data_raw)
                return data

            else:
                message = f"Error when getting inventory items from Amis Web API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as he:
            raise he       
        except RequestException as re:
            error_msg = f"Request exception when getting inventory items from Amis Web API, error: {re}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def get_stocks(self, page: int=1, limit: int=config.AMIS_WEB_PAGE_LIMIT, load_mode: int=3):
        """
        Get inventory items from Amis Web.
        """
        _headers = self.headers
        headers = copy.deepcopy(_headers)
        headers["Referer"] = 'https://actapp.misa.vn/app/DI/DIStock'

        url = f"{config.AMIS_WEB_URL}/g2/api/di/v1/stock/paging_filter"

        payload = json.dumps({
            "sort": "[{\"property\":\"stock_code\",\"desc\":false}]",
            "pageIndex": page,
            "pageSize": limit,
            "useSp": False,
            "view": "view_di_stock",
            "loadMode": load_mode
        })

        try:
            response = self.session.post(url=url, headers=headers, data=payload, timeout=config.AMIS_WEB_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                data = response_json.get("Data", {})
                # data = json.loads(data_raw)
                return data

            else:
                message = f"Error when getting inventory items from Amis Web API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as he:
            raise he       
        except RequestException as re:
            error_msg = f"Request exception when getting inventory items from Amis Web API, error: {re}"
            raise Exception(error_msg)
        except Exception as e:
            raise e