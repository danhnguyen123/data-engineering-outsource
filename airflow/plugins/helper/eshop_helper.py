import requests
import json
import hmac
import hashlib
from requests.exceptions import HTTPError, RequestException
from requests.adapters import HTTPAdapter, Retry
from config import config
from logging import Logger
from helper.redis_helper import RedisHelper, RedisError
import helper.time_helper as TimeHelper

class EshopHelper:

    session = None

    def __init__(self, logger: Logger, redis: RedisHelper):
        self.logger = logger
        self.redis = redis

        if self.session is None:
            self.session = self.request_session()

        self.retries = Retry(total=10,
                backoff_factor=0.5,
                status_forcelist=config.HTTP_CODE_RETRY)
        
        self.session.mount('https://', HTTPAdapter(max_retries=self.retries))
    
    @property
    def headers(self):
        _headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.access_token
        }
        _headers.update({"CompanyCode": self.redis.get_cached_value_for_key(config.ESHOP_COMPANY_CODE_REDIS)})

        self.environment = self.redis.get_cached_value_for_key(config.ESHOP_ENVIRONMENT_REDIS)
        return _headers

    @property
    def get_login_param(self):
        login_time = TimeHelper.get_now_iso_date()
        login_data = {
            "AppID": config.ESHOP_APP_ID,
            "Domain": config.ESHOP_DOMAIN,
            "LoginTime": login_time
        }

        login_data_byte = json.dumps(login_data).replace(" ", "").encode('utf-8')
        secret_key_byte = config.ESHOP_SECRET_KEY.encode('utf-8')
        signature = hmac.new(secret_key_byte, login_data_byte, hashlib.sha256).hexdigest()

        login_param = {
            "AppID": config.ESHOP_APP_ID,
            "Domain": config.ESHOP_DOMAIN,
            "LoginTime": login_time,
            "SignatureInfo": signature
        }

        return login_param
        
    @property
    def access_token(self):
        try:
            _cached_token = self.redis.get_cached_value_for_key(config.ESHOP_ACCESS_TOKEN_REDIS)
            if _cached_token:
                self.logger.debug(f"Get access token from Redis cache: ***")
                return _cached_token
            else:
                _access_token = self._get_access_token()
                self.logger.debug(f"Get access token from Eshop: {_access_token}")
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
        API Doc: https://openplatform.mshopkeeper.vn/api/index.html
        """
        url = f"{config.ESHOP_URL}/auth/api/account/login"
        headers = {
            "Content-Type" : "application/json"
        }
        payload = self.get_login_param

        try:
            response = requests.post(url=url, headers=headers, json=payload, timeout=config.ESHOP_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("ErrorType", 0) == 0:
                data = response_json.get("Data", {})
                access_token = data.get("AccessToken")
                company_code = data.get("CompanyCode")
                environment = data.get("Environment")
                self.redis.put_cached_value_for_key(config.ESHOP_ACCESS_TOKEN_REDIS, access_token, config.ESHOP_ACCESS_TOKEN_TTL)
                self.redis.put_cached_value_for_key(config.ESHOP_COMPANY_CODE_REDIS, company_code)
                self.redis.put_cached_value_for_key(config.ESHOP_ENVIRONMENT_REDIS, environment)

                return access_token

            else:
                message = f"Error when getting access token from Eshop API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting info from Eshop API, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e

    def get_invoices(self, page: int=1, limit: int=config.ESHOP_PAGE_LIMIT, sort_field: str="InvoiceDate", sort_type: int=1, from_datetime: str=None, to_datetime: str=None, date_range_type: int=1):
        """
        Get list invoices from Eshop.

        API Doc: https://openplatform.mshopkeeper.vn/api/invoices_pagingbycustomer.html
        """
        # Headers should be initialized first
        headers = self.headers
        url = f"{config.ESHOP_URL}/{self.environment}/api/v1/invoices/pagingbycustomer"
        payload = json.dumps({
            "Page": page,
            "Limit": limit,
            "SortField": sort_field,
            "SortType": sort_type,
            "FromDate": from_datetime,
            "ToDate": to_datetime,
            "DateRangeType": date_range_type
        })

        try:
            response = self.session.post(url=url, headers=headers, data=payload, timeout=config.ESHOP_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("ErrorType", 0) == 0:
                data = response_json.get("Data")

                return data

            else:
                message = f"Error when getting list invoices from Eshop, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting list invoices from Eshop API, status_code: {response.status_code}, error: {response.text}, error: {e}"
            self.logger.debug(headers)
            self.logger.debug(url)
            self.logger.debug(payload)
            raise Exception(error_msg)
        except Exception as e:
            raise e


    def get_invoice_details(self, invoice_id: str):
        """
        Get list product in a invoice.

        API Doc: https://openplatform.mshopkeeper.vn/api/invoices_pagingbycustomer.html
        """
        # Headers should be initialized first
        headers = self.headers
        url = f"{config.ESHOP_URL}/{self.environment}/api/v1/invoices/detailbyrefid"
        payload = json.dumps({
            "RefID": invoice_id
        })

        try:
            response = self.session.post(url=url, headers=headers, data=payload, timeout=config.ESHOP_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("ErrorType", 0) == 0:
                data = response_json.get("Data", {})

                return data

            else:
                message = f"Error when getting invoice detail from Eshop API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting invoice detail from Eshop API, status_code: {response.status_code}, error: {response.text}, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def get_inventory_items(self, page: int=1, limit: int=config.ESHOP_PAGE_LIMIT, last_sync_date: str=None):
        """
        Get inventory items from Eshop.

        API Doc: https://openplatform.mshopkeeper.vn/api/inventoryitems_pagingwithdetail.html
        """
        # Headers should be initialized first
        headers = self.headers
        url = f"{config.ESHOP_URL}/{self.environment}/api/v1/inventoryitems/pagingwithdetail"
        payload = json.dumps({
            "Page": page,
            "Limit": limit,
            "SortField": "Code",
            "SortType": "1",
            "IncludeInventory": True,
            "InventoryItemCategoryID": None,
            "LastSyncDate": last_sync_date # "2019-10-10 00:00:00"
        })

        try:
            response = self.session.post(url=url, headers=headers, data=payload, timeout=config.ESHOP_REQUEST_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("ErrorType", 0) == 0:
                data = response_json.get("Data")

                return data

            else:
                message = f"Error when getting inventory items from Eshop, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting inventory items from Eshop API, status_code: {response.status_code}, error: {response.text}, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e