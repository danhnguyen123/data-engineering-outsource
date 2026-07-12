from typing import Dict, List, Optional, Type
import requests
import json
from requests.exceptions import HTTPError, RequestException
from urllib import request, parse
from config import config
from logging import Logger

class HubspotHelper:

    session = None

    def __init__(self, logger: Logger, redis=None):
        self.logger = logger
        self.redis = redis

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
            "Authorization": "Bearer " + self.access_token
        }

    @property
    def access_token(self):
        # Prefer the short-lived token cached in Redis (produced by the HubSpot CLI
        # refresh task), fall back to the static private-app token from env.
        if self.redis:
            token = self.redis.get_cached_value_for_key(config.HUBSPOT_ACCESS_TOKEN_REDIS)
            if token:
                return token
        return config.HUBSPOT_APP_TOKEN
    
    def list_contacts(self, limit=None, archived="false", after=None, properties: list = None, associations: list = None):
        params = {
            "limit": limit if limit else config.HUBSPOT_PAGE_SIZE
            }
        if archived:
            params.update({"archived": archived}) 
        if after:
            params.update({"after": after}) 
        if properties:
            properties = ','.join(i for i in properties)
            params.update({"properties": properties}) 
        if associations:
            associations = ','.join(i for i in associations)
            params.update({"associations": associations}) 

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/contacts" 

        try:
            response = self.session.get(url=url, headers=self.headers, params=params, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when listting contacts record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when listting contacts record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def search_contacts(self, limit=None, after=None, properties: list = None, filterGroups=None):
        payload = {
            "limit": str(limit) if limit else config.HUBSPOT_PAGE_SIZE
            }
        if after:
            payload.update({"after": after}) 
        if properties:
            payload.update({"properties": properties}) 
        if filterGroups:
            payload.update({"filterGroups": filterGroups})    

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when searching contacts record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when searching contacts record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def list_deals(self, limit=None, archived="false", after=None, properties: list = None, associations: list = None):
        params = {
            "limit": limit if limit else config.HUBSPOT_PAGE_SIZE
            }
        if archived:
            params.update({"archived": archived}) 
        if after:
            params.update({"after": after}) 
        if properties:
            properties = ','.join(i for i in properties)
            params.update({"properties": properties}) 
        if associations:
            associations = ','.join(i for i in associations)
            params.update({"associations": associations}) 

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/deals" 

        try:
            response = self.session.get(url=url, headers=self.headers, params=params, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when listting deals record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when listting deals record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def search_deals(self, limit=None, after=None, properties: list = None, filterGroups=None):
        payload = {
            "limit": str(limit) if limit else config.HUBSPOT_PAGE_SIZE
            }
        if after:
            payload.update({"after": after}) 
        if properties:
            payload.update({"properties": properties}) 
        if filterGroups:
            payload.update({"filterGroups": filterGroups})    

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/deals/search" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when searching deals record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when searching deals record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e

    def list_tickets(self, limit=None, archived="false", after=None, properties: list = None, associations: list = None):
        params = {
            "limit": limit if limit else config.HUBSPOT_PAGE_SIZE
            }
        if archived:
            params.update({"archived": archived}) 
        if after:
            params.update({"after": after}) 
        if properties:
            properties = ','.join(i for i in properties)
            params.update({"properties": properties}) 
        if associations:
            associations = ','.join(i for i in associations)
            params.update({"associations": associations}) 

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/tickets" 

        try:
            response = self.session.get(url=url, headers=self.headers, params=params, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when listting tickets record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when listting tickets record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def search_tickets(self, limit=None, after=None, properties: list = None, filterGroups=None):
        payload = {
            "limit": str(limit) if limit else config.HUBSPOT_PAGE_SIZE
            }
        if after:
            payload.update({"after": after}) 
        if properties:
            payload.update({"properties": properties}) 
        if filterGroups:
            payload.update({"filterGroups": filterGroups})    

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/tickets/search" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when searching tickets record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when searching tickets record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e

    def list_feedback_submissions(self, limit=None, archived="false", after=None, properties: list = None, associations: list = None):
        params = {
            "limit": limit if limit else config.HUBSPOT_PAGE_SIZE
            }
        if archived:
            params.update({"archived": archived}) 
        if after:
            params.update({"after": after}) 
        if properties:
            properties = ','.join(i for i in properties)
            params.update({"properties": properties}) 
        if associations:
            associations = ','.join(i for i in associations)
            params.update({"associations": associations}) 

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/feedback_submissions" 

        try:
            response = self.session.get(url=url, headers=self.headers, params=params, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when listting Feedback Submissions record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when listting Feedback Submissions record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e
        
    def search_feedback_submissions(self, limit=None, after=None, properties: list = None, filterGroups=None):
        payload = {
            "limit": str(limit) if limit else config.HUBSPOT_PAGE_SIZE
            }
        if after:
            payload.update({"after": after}) 
        if properties:
            payload.update({"properties": properties}) 
        if filterGroups:
            payload.update({"filterGroups": filterGroups})    

        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/feedback_submissions/search" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json.get("results",[])
                after = response_json.get("paging", {}).get("next", {}).get("after", None)

                return results, after

            else:
                message = f"Error when searching Feedback Submissions record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when searching Feedback Submissions record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e

    def read_associations(self, from_object, to_object, inputs):
        payload = {
            "inputs": inputs
            }

        url = f"{config.HUBSPOT_BASE_URL}/crm/v4/associations/{from_object}/{to_object}/batch/read" 

        try:
            payload = json.dumps(payload)
            response = self.session.post(url=url, headers=self.headers, data=payload, timeout=config.HUBSPOT_API_TIMEOUT)
            response_json = response.json()

            if response.status_code == 200 or response.status_code == 207:
                results = response_json.get("results",[])
                return results
            
            else:
                message = f"Error when searching associations record from HupSpot, status_code: {response.status_code}, error: {response.text}"
                self.logger.error(message)
                raise HTTPError(message)

        except HTTPError as e:
            raise e       
        except RequestException as e:
            error_msg = f"Request exception when searching associations record from HupSpot, error: {e}"
            raise Exception(error_msg)
        except Exception as e:
            raise e

    def merge_association_to_search_result(self, from_object: str, to_object: str, association_type_id: int, results):
        list_id = [result.get("id") for result in results]
        inputs = []
        for id in list_id:
            inputs.append({
                "id": f"{id}"
            })
        association_results = self.read_associations(from_object=from_object, to_object=to_object, inputs=inputs)
        object_to_contact = {}
        for association in association_results:
            from_id = association.get("from", {}).get("id")

            for i in association.get("to", []):
                for j in i.get("associationTypes", []):
                    if j.get("typeId") == association_type_id: #https://developers.hubspot.com/docs/api/crm/associations#association-type-id-values
                        result_association = {
                            from_id: {
                                to_object : {
                                    "results": [
                                        {
                                            "id": str(i.get("toObjectId")),
                                            "type": f"{from_object[0:-1]}_to_{to_object[0:-1]}"
                                        }
                                    ]
                                }
                            }
                        }
                        object_to_contact.update(result_association)

        for i in results:
            id = i.get("id")
            if id in object_to_contact:
                i.update({"associations": object_to_contact.get(id)})
                
        return results
