# -*- coding: utf-8 -*-

"""

Discovergy Awattar module

Poll the Awattar API
"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode

import arrow  # type: ignore
import httpx
import numpy as np  # type: ignore
import pandas as pd  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential  # type: ignore

from .utils import before_log, split_df_by_month, write_data_frames


async def get(*, config: Box) -> None:
    """Fetch and write Awattar data."""
    start_ts = arrow.utcnow()
    try:
        data = await get_data(config=config)
    except Exception as e:
        log.warning("Could not fetch Awattar data: {}.".format(str(e)))
    else:
        elapsed_time = arrow.utcnow() - start_ts
        log.debug(f"Fetching Awattar data took {elapsed_time.total_seconds():.3f} s.")

    data_frames = split_df_by_month(df=raw_to_df(data=data))
    # for df in data_frames:
    #     # Check if there are changed values. This should not happen.
    #     joined = df.join(df_prev, how="outer", lsuffix="l", rsuffix="r")
    #     if not compare(joined.iloc[:, 0].values, joined.iloc[:, 1].values):
    #         log.warning(f"Found inconsistent data in {name} data. See debug log.")
    #         log.debug(f"{df}")
    #         log.debug(f"{df_prev}")
    write_data_frames(config=config, data_frames=data_frames, name="awattar")


@retry(
    before=before_log(log, "debug"),
    stop=(stop_after_delay(10) | stop_after_attempt(5)),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
async def get_data(
    *, config: Box, start: Optional[int] = None, end: Optional[int] = None
) -> Union[Dict[Any, Any], List[Any]]:
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


def raw_to_df(*, data: Dict) -> pd.DataFrame:
    """Return the raw Awattar data as a Pandas DataFrame."""
    date_index = (pd.Timestamp(e["start_timestamp"], unit="ms", tz="utc") for e in data["data"] if "start_timestamp" in e)
    prices = (e['marketprice'] for e in data['data'] if 'marketprice' in e)
    df = pd.DataFrame(prices, index=date_index, columns=["marketprice"])
    return df


def compare(s1: pd.Series, s2: pd.Series) -> bool:
    """Compare 2 Pandas Series. Either any of the 2 elements is NaN or both are equal."""
    s1, s2 = np.asarray(s1), np.asarray(s2)
    (s1 == s2) | (np.isnan(s1) & np.isnan(s2))
    return ((s1 == s2) | (np.isnan(s1) | np.isnan(s2))).all()
