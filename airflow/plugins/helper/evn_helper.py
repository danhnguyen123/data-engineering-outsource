"""
Helper to query EVN SPC power-outage schedule (Lịch ngừng giảm cung cấp điện).

Two lookup modes (ChucNang):
  - MaKhachHang: by customer code   -> param maKH
  - MaDonVi:     by unit code       -> param madvi

The endpoint returns an HTML fragment. A browser-like session warm-up (GET the
referer page first) is used to obtain cookies, and the request is retried a few
times because the first call can transiently fail.
"""
import time
import requests
from requests.exceptions import RequestException
from logging import Logger


class EVNHelper:

    BASE_URL = "https://cskh.evnspc.vn/TraCuu/GetThongTinLichNgungGiamCungCapDien"
    REFERER_URL = "https://cskh.evnspc.vn/TraCuu/LichNgungGiamCungCapDien"

    def __init__(self, logger: Logger, timeout: int = 30, retries: int = 5, retry_delay: int = 3):
        self.logger = logger
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay

    @property
    def headers(self):
        return {
            "Accept": "text/html, */*; q=0.01",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Referer": self.REFERER_URL,
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"),
            "X-Requested-With": "XMLHttpRequest",
        }

    def get_outage_html(self, chuc_nang: str, code: str, tu_ngay: str, den_ngay: str) -> str:
        """
        Fetch the outage-schedule HTML fragment.

        :param chuc_nang: "MaKhachHang" or "MaDonVi"
        :param code: customer code (maKH) or unit code (madvi)
        :param tu_ngay: from date, format DD-MM-YYYY
        :param den_ngay: to date, format DD-MM-YYYY
        :return: raw HTML string
        """
        params = {"tuNgay": tu_ngay, "denNgay": den_ngay, "ChucNang": chuc_nang}
        if chuc_nang == "MaDonVi":
            params["madvi"] = code
        else:
            params["maKH"] = code

        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                session = requests.Session()
                session.headers.update(self.headers)
                # Warm up to obtain cookies like a browser would
                session.get(self.REFERER_URL, timeout=self.timeout)
                resp = session.get(self.BASE_URL, params=params, timeout=self.timeout)

                if resp.status_code == 200 and resp.text.strip():
                    return resp.text

                last_err = f"status_code={resp.status_code}, empty_body={not resp.text.strip()}"
                self.logger.warning(f"EVN attempt {attempt}/{self.retries} failed: {last_err}")
            except RequestException as e:
                last_err = str(e)
                self.logger.warning(f"EVN attempt {attempt}/{self.retries} error: {e}")

            if attempt < self.retries:
                time.sleep(self.retry_delay)

        raise Exception(f"EVN request failed after {self.retries} attempts (chuc_nang={chuc_nang}, code={code}): {last_err}")
