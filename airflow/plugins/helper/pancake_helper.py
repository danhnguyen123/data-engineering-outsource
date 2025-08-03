import requests
import json
import hmac
import hashlib
from requests.exceptions import HTTPError, RequestException
from requests.adapters import HTTPAdapter, Retry
from config import config
from logging import Logger

class PancakeHelper:

    session = None

    def __init__(self, logger: Logger,):
        self.logger = logger
        self.headers = {}

        if self.session is None:
            self.session = self.request_session()

    def request_session(self):
        """
        Create a session with retry handling

        :param session (_type_, optional): user session.
        :returns session: request session
        """
        if self.session is None:
            session = requests.Session()
        return session
    
    def get_page_customer(self, page_access_token, page_id, since, until, page_number=1, page_size=100, order_by="updated_at"):

        url = f"https://pages.fm/api/public_api/v1/pages/{page_id}/page_customers"
        params = {
            "page_access_token": page_access_token,
            "since": since,
            "until": until,
            "page_number": page_number,
            "page_size": page_size,
            "order_by": order_by
        }
        try:
            response = self.session.get(url=url, params=params, timeout=config.PANCAKE_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                data = response_json.get("customers")
                return data

            else:
                message = f"Error when getting list customer from Pancake, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)
            
        except Exception as e:
            raise e


    def get_conversations(self, page_access_token, page_id, last_conversation_id=None, since=None, until=None, order_by="updated_at"):
        '''
        https://docs.pancake.biz/pancake/st-f12/st-p2?lang=vi#103388f8-a942-4754-8504-fb001065c423
        '''

        url = f"https://pages.fm/api/public_api/v1/pages/{page_id}/page_customers"
        params = {
            "page_access_token": page_access_token,
            "since": since,
            "until": until,
            "order_by": order_by
        }
        if last_conversation_id:
            params.update({"last_conversation_id": last_conversation_id})
        if since:
            params.update({"since": since})
        if until:
            params.update({"until": until})

        try:
            response = self.session.get(url=url, params=params, timeout=config.PANCAKE_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                data = response_json.get("conversations")
                return data

            else:
                message = f"Error when getting list conversations from Pancake, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)
            
        except Exception as e:
            raise e