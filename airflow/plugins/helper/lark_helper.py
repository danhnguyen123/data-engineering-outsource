import requests
import json
from requests.exceptions import HTTPError, RequestException
from urllib import request, parse
from config import config
from logging import Logger
from helper.redis_helper import RedisHelper, RedisError

class LarkHelper:

    session = None

    def __init__(self, logger: Logger, redis: RedisHelper):
        self.logger = logger
        self.redis = redis
        self.session = self.request_session()
    
    @property
    def headers(self):
        return {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.tenant_access_token
        }

    @property
    def tenant_access_token(self):
        try:
            cached_token = self.redis.get_cached_value_for_key(config.LARK_TOKEN_REDIS_KEY)
            if cached_token:
                self.logger.debug(f"Get tenant_access_token from Redis: {cached_token}")
                return cached_token
            else:
                tenant_access_token = self._get_tenant_access_token()
                self.logger.debug(f"Get tenant_access_token from Lark: {tenant_access_token}")
                return tenant_access_token
            
        except RedisError as e:
            raise e
        except HTTPError as e:
            raise e
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

    def _get_tenant_access_token(self):
        url = f"{config.LARK_OPEN_URL}/open-apis/auth/v3/tenant_access_token/internal"
        headers = {
            "Content-Type" : "application/json"
        }
        payload = {
            "app_id": config.LARK_APP_ID,
            "app_secret": config.LARK_APP_SECRET
        }

        try:
            response = requests.post(url=url, headers=headers, json=payload, timeout=30)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("code") == 0:
                tenant_access_token = response_json.get("tenant_access_token")
                self.redis.put_cached_value_for_key(config.LARK_TOKEN_REDIS_KEY, tenant_access_token, config.LARK_TOKEN_REDIS_TTL)
                return tenant_access_token

            else:
                message = f"Error when getting tenant_access_token from Lark API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting info from Lark API, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e


class LarkMessage(LarkHelper):

    def __init__(self, logger: Logger, redis: RedisHelper):
        super().__init__(logger=logger, redis=redis)
        self.logger = logger
        self.redis = redis

    def send_message(self, receiver, content):
        # receiver = {'type':'email','id':'foo@bar.com'}
        # content = {'type':'text','content':'hihi'}
        url = f"{config.LARK_OPEN_URL}/open-apis/im/v1/messages?receive_id_type={receiver['type']}"

        content_json = {
            content['type']: content['content']
        }

        req_body = {
            "receive_id": receiver['id'],
            "msg_type": content['type'],
            "content": json.dumps(content_json)
        }

        req_body

        payload = json.dumps(req_body)

        try:
            response = requests.post(url=url, headers=self.headers, data=payload, timeout=config.LARK_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("code") == 0:
                return "Success"

            else:
                message = f"Error when sending message to Lark API, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when sending message to Lark, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e

class LarkBase(LarkHelper):

    def __init__(self, logger: Logger, redis: RedisHelper):
        super().__init__(logger=logger, redis=redis)

    
    def list_records(self, app_token, table_id, page_size=None, page_token=None, field_names=None):
        params = {
            "page_size": page_size if page_size else config.LARK_BASE_PAGE_SIZE
            }
        if page_token:
            params.update({"page_token": page_token}) 
        if field_names:
            params.update({"field_names": field_names}) 

        url = f"{config.LARK_OPEN_URL}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"

        try:
            response = self.session.get(url=url, headers=self.headers, params=params, timeout=config.LARK_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("code") == 0:
                data = response_json.get("data",{})
                has_more = data.get("has_more")
                items = data.get("items",[])
                page_token = data.get("page_token")
                number_of_records = data.get("total")

                return has_more, items, page_token, number_of_records

            else:
                message = f"Error when getting record from Lark Base, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting record from Lark Base, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def create_records(self, app_token, table_id, payload):
        
        payload = json.dumps(payload)

        url = f"{config.LARK_OPEN_URL}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"

        try:
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.LARK_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 and response_json.get("code") == 0:
                return response_json
            else:
                message = f"Error when creating record to Lark Base, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when creating record to Lark Base, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e