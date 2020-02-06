# -*- coding: utf-8 -*-

"""Discovergy poller

Poll for data

"""

import gzip
import json
import os
import sys
import time

from functools import lru_cache
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import arrow  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log
from pyowm import OWM  # type: ignore

from .api import DiscovergyMeter, describe_meters, save_meters
from .config import read_config
from .utils import start_logging


@lru_cache(maxsize=50)
def get_weather(
    *, latitude: float, longitude: float, owm_id: str, ttl_hash: int
) -> dict:
    """Return the weather data."""
    open_weather_map = OWM(owm_id)
    if not open_weather_map.is_API_online():
        log.warning("Open Weather Map endpoint is not online-line.\n")
        return {}
    try:
        weather = open_weather_map.weather_at_coords(latitude, longitude)
    except Exception as e:
        sys.stderr.write("Could not fetch weather: {}.\n".format(str(e)))
        log.warning("Could not fetch weather: {}.\n".format(str(e)))
        return {}

    return json.loads(weather.to_JSON())


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


def write_data(*, data: List[Dict], file_path: Path) -> None:
    """Write the gzipped data to file_path."""
    dst_dir = file_path.parent
    if not dst_dir.expanduser().is_dir():
        log.warning(f"Creating the data destination directory {dst_dir}.")
        os.makedirs(dst_dir.expanduser().as_posix())

    with gzip.open(file_path.expanduser().as_posix(), "wb") as fh:
        fh.write(json.dumps(data).encode("utf-8"))


def read_data(
    *,
    config: Box,
    meters: Dict[str, DiscovergyMeter],
    date_from: arrow.Arrow,
    date_to: arrow.Arrow,
) -> None:
    """Poll the Discovergy API."""

    for meter_id, meter in meters.items():
        log.info("Fetching data for meter {meter_id}...")
        data = meter.readings(
            ts_from=date_from.timestamp, ts_to=date_to.timestamp, resolution="raw"
        )
        log.info(f"To get data for meter {meter_id} took {meter.last_query_duration}.")
        file_name = (
            f"discovergy_data_{date_from.format('YYYY-MM-DD_HH-mm-ss')}_"
            f"{date_to.format('YYYY-MM-DD_HH-mm-ss')}.json.gz"
        )
        file_path = Path(config.file_location.data_dir) / Path(file_name)
        write_data(data=data, file_path=file_path)


def main(config: Box) -> None:
    meters = get_meters(config)
    read_interval = timedelta(hours=12)
    date_to = arrow.utcnow()
    date_from = date_to - read_interval
    while True:
        try:
            read_data(
                config=config, meters=meters, date_from=date_from, date_to=date_to
            )
        except KeyboardInterrupt:
            log.info("Polling Discovergy was ended by <Ctrl>+<C>.")
            sys.exit()
        except Exception as e:
            log.error("Error in poller: {}.\n".format(str(e)))
            time.sleep(15)
        else:
            time.sleep(read_interval.seconds)
            date_from = date_to
            date_to = arrow.utcnow()


if __name__ == "__main__":
    config = read_config()
    start_logging(config)
    main(config)
