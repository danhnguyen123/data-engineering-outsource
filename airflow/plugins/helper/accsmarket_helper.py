"""
Helper to fetch AccsMarket catalog pages (server-rendered HTML, all items on one page).
"""
import time
import requests
from requests.exceptions import RequestException
from logging import Logger


class AccsMarketHelper:

    BASE_URL = "https://www.accsmarket.com"

    def __init__(self, logger: Logger, timeout: int = 60, retries: int = 5, retry_delay: int = 5):
        self.logger = logger
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay

    @property
    def headers(self):
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
        }

    def get_catalog_html(self, path: str) -> str:
        """
        Fetch a catalog page.

        :param path: catalog path, e.g. "/en/catalog/gmail/s-otlezhkoj-25"
        :return: raw HTML string
        """
        url = f"{self.BASE_URL}{path}"
        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, headers=self.headers, timeout=self.timeout)
                has_items = 'class="soc-body' in resp.text
                if resp.status_code == 200 and has_items:
                    return resp.text

                last_err = f"status_code={resp.status_code}, has_items={has_items}"
                self.logger.warning(f"AccsMarket attempt {attempt}/{self.retries} failed ({url}): {last_err}")
            except RequestException as e:
                last_err = str(e)
                self.logger.warning(f"AccsMarket attempt {attempt}/{self.retries} error ({url}): {e}")

            if attempt < self.retries:
                time.sleep(self.retry_delay)

        raise Exception(f"AccsMarket request failed after {self.retries} attempts ({url}): {last_err}")
