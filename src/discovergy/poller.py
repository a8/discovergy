# -*- coding: utf-8 -*-

"""Discovergy poller

Poll for data from different sources.

All functions that end with _task will be feed to the event loop.
"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import asyncio
import re
import sys

from datetime import timedelta
from pathlib import Path
from typing import Dict

import arrow  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log

from . import awattar, weather
from .api import DiscovergyMeter, describe_meters, save_meters
from .config import read_config
from .utils import start_logging, write_data


async def get_awattar(*, config: Box) -> None:
    """Fetch and write Awattar data."""
    start_ts = arrow.utcnow()
    try:
        data = await awattar.get_data(config=config)
    except Exception as e:
        log.warning("Could not fetch Awattar data: {}.".format(str(e)))
    else:
        elapsed_time = arrow.utcnow() - start_ts
        log.debug(f"Fetching Awattar data took {elapsed_time}.")

    date = arrow.utcnow()
    file_name = f"awattar_{date.format('YYYY-MM-DD_HH-mm-ss')}.json.gz"
    file_path = Path(config.file_location.data_dir) / Path(file_name)
    write_data(data=data, file_path=file_path)


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


def read_data(
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
        log.info(f"To get data for meter {meter_id} took {meter.last_query_duration}.")
        file_name = (
            f"discovergy_data_{date_from.format('YYYY-MM-DD_HH-mm-ss')}_"
            f"{date_to.format('YYYY-MM-DD_HH-mm-ss')}.json.gz"
        )
        file_path = Path(config.file_location.data_dir) / Path(file_name)
        write_data(data=data, file_path=file_path)


async def discovergy_meter_read_task(
    *, config: Box, loop: asyncio.base_events.BaseEventLoop,
) -> None:
    """Async worker to poll the Discovergy API."""
    meters = get_meters(config)
    read_interval = timedelta(seconds=int(config.poll.discovergy))
    date_to = arrow.utcnow()
    date_from = date_to - read_interval
    log.debug(f"The Discovergy read interval is {read_interval}.")
    while loop.is_running():
        try:
            # FIXME (a8): This isn't an async call yet because we use requests. OTHO, this doesn't
            # really matter ATM. We only call Discovergy every few hours.
            read_data(
                config=config, meters=meters, date_from=date_from, date_to=date_to
            )
        except Exception as e:
            log.warning(
                "Error in Discovergy poller. Retrying in 15 seconds. {}".format(str(e))
            )
            await asyncio.sleep(15)
        else:
            await asyncio.sleep(read_interval.seconds)
            date_from = date_to
            date_to = arrow.utcnow()


async def awattar_read_task(
    *, config: Box, loop: asyncio.base_events.BaseEventLoop,
) -> None:
    """Async worker to poll the Open Weather Map API."""
    read_interval = timedelta(seconds=int(config.poll.awattar))
    log.debug(f"The Awattar read interval is {read_interval}.")
    while loop.is_running():
        try:
            await get_awattar(config=config)
        except Exception as e:
            log.warning(
                "Error in Open Weather Map poller. Retrying in 15 seconds. {}".format(
                    str(e)
                )
            )
            await asyncio.sleep(15)
        else:
            await asyncio.sleep(read_interval.seconds)


async def open_weather_map_read_task(
    *, config: Box, loop: asyncio.base_events.BaseEventLoop,
) -> None:
    """Async worker to poll the Open Weather Map API."""
    read_interval = timedelta(seconds=int(config.poll.weather))
    log.debug(f"The Open Weather Map read interval is {read_interval}.")
    while loop.is_running():
        try:
            # FIXME (a8): This isn't an async call yet because we use requests.
            weather.get_open_weather_map(config=config)
        except Exception as e:
            log.warning(
                "Error in Open Weather Map poller. Retrying in 15 seconds. {}".format(
                    str(e)
                )
            )
            await asyncio.sleep(15)
        else:
            await asyncio.sleep(read_interval.seconds)


def main(config: Box) -> None:
    loop = asyncio.get_event_loop()
    task_match = re.compile(r"^.*_task$")
    # Add all tasks to the event loop.
    for attr in globals().keys():
        if task_match.match(attr):
            asyncio.ensure_future(globals()[attr](config=config, loop=loop))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        log.info("Polling Discovergy and friends was ended by <Ctrl>+<C>.")
        sys.exit(0)
    except Exception as e:
        log.error(f"While running the poller event loop we caught {e}.")
    finally:
        print("Closing event loop")
        loop.close()


if __name__ == "__main__":
    config = read_config()
    start_logging(config)
    main(config)
