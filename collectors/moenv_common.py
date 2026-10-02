"""環境部 data.moenv.gov.tw API v2 共用分頁抓取（water_effluent_monitoring / cems_stack_monitoring）。

⚠ data.moenv.gov.tw 憑證缺 Subject Key Identifier → verify=False（同台電/健保署）。
⚠ API key 只放 params，不得出現在 log／例外訊息（requests 例外字串含完整 URL → 一律改寫）。
"""

from __future__ import annotations

import time
from typing import Callable, Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MOENV_API_BASE = "https://data.moenv.gov.tw/api/v2"


class MoenvFetchError(RuntimeError):
    pass


def new_session(tag: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": f"Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; {tag})"})
    s.verify = False
    return s


def fetch_pages(
    session: requests.Session,
    code: str,
    api_key: str,
    *,
    page_size: int = 1000,
    max_pages: int = 10,
    timeout: int = 60,
    retries: int = 2,
    stop_when: Optional[Callable[[list[dict]], bool]] = None,
) -> list[dict]:
    """抓 MOENV dataset 前 N 頁（上游預設排序為最新在前）。

    stop_when(page_records) 回 True 即停止翻頁（例如本頁最舊一筆已早於回看視窗）。
    錯誤訊息只帶 dataset code，不帶 URL（避免洩漏 api_key）。
    """
    out: list[dict] = []
    for page in range(max_pages):
        params = {"api_key": api_key, "limit": page_size, "offset": page * page_size, "format": "JSON"}
        last_err: Optional[str] = None
        records = None
        for attempt in range(retries + 1):
            try:
                resp = session.get(f"{MOENV_API_BASE}/{code}", params=params, timeout=timeout)
                if resp.status_code != 200:
                    last_err = f"HTTP {resp.status_code}"
                else:
                    payload = resp.json()
                    records = payload.get("records") if isinstance(payload, dict) else payload
                    if not isinstance(records, list):
                        last_err = "unexpected payload shape"
                        records = None
                    else:
                        break
            except (requests.RequestException, ValueError) as e:
                last_err = type(e).__name__
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
        if records is None:
            raise MoenvFetchError(f"{code} page {page} failed: {last_err}")
        out.extend(records)
        if len(records) < page_size:
            break
        if stop_when and stop_when(records):
            break
    return out
