from typing import Dict, List, Optional, Type
import requests
import json
from requests.exceptions import HTTPError, RequestException
from urllib import request, parse
from config import config
from logging import Logger

class DahahiHelper:

    session = None

    def __init__(self, logger: Logger):
        self.logger = logger

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

    @property
    def headers(self):
        return {
            "Content-Type": "application/json",
            "AppKey": config.DAHAHI_APP_KEY,
            "SecretKey": config.DAHAHI_SECRET_KEY,
        }

    def get_checkin_history(self, page_size=None, page_index=None, from_time_string=None, to_time_string=None):
        payload = {
            "pagesize": page_size if page_size else config.DAHAHI_PAGE_SIZE,
            "pageIndex": page_index
            }
        
        if from_time_string:
            payload.update({"FromTimeStr": from_time_string}) 
        if to_time_string:
            payload.update({"ToTimeStr": to_time_string}) 
 
        url = f"{config.DAHAHI_BASE_URL}/api/facereg/checkinhis" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.DAHAHI_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("Data",[])
                print(results)

                return results

            else:
                message = f"Error when getting checkin history record from Dahahi, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting checkin history record from Dahahi, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def get_employee_list(self, page_size=None, page_index=None):
        payload = {
            "pagesize": page_size if page_size else config.DAHAHI_PAGE_SIZE,
            "pageIndex": page_index if page_index else 1
            }
        
        url = f"{config.DAHAHI_BASE_URL}/api/facereg/GetEmployeeList" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.DAHAHI_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("Data",[])
                return results

            else:
                message = f"Error when getting employee record from Dahahi, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when getting employee record from Dahahi, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e