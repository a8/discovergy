# -*- coding: utf-8 -*-

"""

Discovergy Awattar module

Poll the Awattar API
"""
from urllib.parse import urlencode
from typing import Dict, Optional

import httpx

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential  # type: ignore

from .utils import before_log


@retry(
    before=before_log(log, "debug"),
    stop=(stop_after_delay(10) | stop_after_attempt(5)),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
async def get_data(
    *, config: Box, start: Optional[int] = None, end: Optional[int] = None
) -> Dict:
    """Return a new consumer token."""
    endpoint = "https://api.awattar.de/v1/marketdata{}".format
    params = {}
    if start:
        params["start"] = str(start)
    if end:
        params["end"] = str(end)
    if params:
        url = endpoint("?" + urlencode(params))
    else:
        url = endpoint("")
    timeout = 10.0

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=timeout)
    except Exception as e:
        log.error(f"Caught an exception while fetching data from the Awattar API: {e}")
        raise
    try:
        data = response.json()
    except Exception as e:
        log.error(f"Could not JSON decode the Awattar response: {e}")
        raise

    return data
