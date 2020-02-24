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
import pandas as pd  # type: ignore
import schema  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential  # type: ignore

from .api import DiscovergyMeter, describe_meters, save_meters
from .utils import before_log, split_df_by_day, write_data_frames, write_data_to_pystore


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
        write_data_to_pystore(
            config=config,
            data_frames=split_df_by_day(df=df),
            name=f"power_{meter_id}",
            metadata={"meter_id": meter_id},
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
    """Return the raw Discovergy power meter data as a Pandas DataFrame.

    The index is re-sampled to full seconds. The Discovergy API returns values
    at about a rate of 1 second.
    """
    index = []
    values = defaultdict(list)
    for reading in data:
        try:
            ValueSchema.validate(reading["values"])
        except schema.SchemaError as e:
            log.warning(f"Got invalid data from Discovergy: {e}")
            continue
        index.append(pd.Timestamp(reading["time"], unit="ms"))
        # index.append(pd.Timestamp(reading["time"], unit="ms", tz="utc"))
        for k, v in reading["values"].items():
            # Saving tons of 0s from using disk space.
            # Watt resolution is all we need and Discovergy reports anyway.
            if k.startswith("energy") and v > 0:
                v = int(v / 10000000)
            # Do not store a higher precision than we get. This is mV resolution.
            elif k.startswith("voltage") and v > 0:
                v = int(v / 100)
            else:
                values[k].append(int(v))
    df = pd.DataFrame(values, index=index)
    del values
    # The Discovergy API returns data at ~1s intervals. Resample to full seconds.
    df = pd.DataFrame(df.resample("1s").median())
    return df


def data_from_files(data_dir, meter_id):
    """Read data from raw data dumped JSON files."""
    from .config import read_config
    import gzip
    import os
    import json
    import pystore
    from pathlib import Path

    config = read_config()
    name = f"power_{meter_id}"
    pystore.set_path(Path(config.file_location.data_dir).expanduser().as_posix())
    for f in sorted(os.listdir(data_dir)):
        if f.startswith("discovergy_data"):
            with gzip.open(f"{data_dir}/{f}") as fh:
                data = json.load(fh)
            df = raw_to_df(data=data)
            print(f)
            write_data_to_pystore(
                config=config,
                data_frames=split_df_by_day(df=df),
                name=name,
                metadata={"meter_id": meter_id},
            )
