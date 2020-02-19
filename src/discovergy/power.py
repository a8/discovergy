# -*- coding: utf-8 -*-

"""

Discovergy Power Meters module

"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import sys

from collections import defaultdict
from typing import Dict, List

import arrow  # type: ignore
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import schema  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential  # type: ignore

from .api import DiscovergyMeter, describe_meters, save_meters
from .utils import before_log, split_df_by_month, write_data_frames


ValueSchema = schema.Schema(
    {
        "energyOut": int,
        "energy2": int,
        "energy1": int,
        "voltage1": int,
        "voltage2": int,
        "voltage3": int,
        "energyOut1": int,
        "power": int,
        "energyOut2": int,
        "power3": int,
        "power1": int,
        "energy": int,
        "power2": int,
    }
)


# import pysnooper
# @pysnooper.snoop()
def get(
    *,
    config: Box,
    meters: Dict[str, DiscovergyMeter],
    date_from: arrow.Arrow,
    date_to: arrow.Arrow,
) -> None:
    """Poll the Discovergy API."""
    for meter_id, meter in meters.items():
        log.info(f"Fetching data for meter {meter_id}...")
        data = meter.readings(
            ts_from=date_from.timestamp, ts_to=date_to.timestamp, resolution="raw"
        )
        log.info(
            f"To get data for meter {meter_id} took {meter.last_query_duration:.3f} s."
        )
        df = raw_to_df(data=data)
        write_data_frames(
            config=config,
            data_frames=split_df_by_month(df=df),
            name=f"power_{meter_id}",
        )


@retry(
    before=before_log(log, "debug"),
    stop=(stop_after_delay(10) | stop_after_attempt(5)),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
def get_meters(config: Box) -> Dict[str, DiscovergyMeter]:
    """Describe all meters, save them to the config dir, and return the meters
    configured. In no [meters] are configured return all."""
    if "meters" in config:
        configured_meters = config.meters.values()
    else:
        configured_meters = []
    meters = {}
    now = arrow.utcnow()
    for meter in describe_meters(config):
        meter_id = meter.get("meterId")
        meter["timestamp"] = now.timestamp
        if not meter_id:
            log.error(
                "Got the following meter metadata from the Discovergy API lacking a meter id (meterId): {meter}."
            )
            sys.exit(1)
        if not configured_meters or meter_id in configured_meters:
            meters[meter_id] = DiscovergyMeter(meter=meter, config=config)
    save_meters(config=config, meters={m.meter_id: m.metadata for m in meters.values()})

    return meters


def raw_to_df(*, data: List[Dict]) -> pd.DataFrame:
    """Return the raw Discovergy power meter data as a Pandas DataFrame."""
    index = []
    values = defaultdict(list)
    for reading in data:
        try:
            ValueSchema.validate(reading["values"])
        except schema.SchemaError as e:
            log.warning(f"Got invalid data from Discovergy: {e}")
            continue
        index.append(pd.Timestamp(reading["time"], unit="ms", tz="utc"))
        for k, v in reading["values"].items():
            # Saving tons of 0s from using disk space. Cutting of 10^7 allows us
            # to save in dtype=np.int32 which is about half of int64 or float 64.
            # Watt resolution is all we need and Discovergy reports anyway.
            if k.startswith("energy") and v > 100:
                v = int(v / 10000000)
            # Do not store a higher precision than we get. This is mV resolution.
            if k.startswith("voltage") and v > 0:
                v = int(v / 100)
            values[k].append(v)
    df = pd.DataFrame(values, index=index, dtype=np.int32)
    return df
