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

import arrow  # type: ignore
import pystore

from box import Box  # type: ignore
from loguru import logger as log

from . import awattar, power, weather
from .config import read_config
from .utils import start_logging


async def discovergy_meter_read_task(
    *, config: Box, loop: asyncio.base_events.BaseEventLoop,
) -> None:
    """Async worker to poll the Discovergy API."""
    meters = power.get_meters(config)
    read_interval = timedelta(seconds=int(config.poll.discovergy))
    date_to = arrow.utcnow()
    date_from = date_to - read_interval
    log.debug(f"The Discovergy read interval is {read_interval}.")
    while loop.is_running():
        try:
            # FIXME (a8): This isn't an async call yet because we use requests. OTHO, this doesn't
            # really matter ATM. We only call Discovergy every few hours.
            power.get(
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
            await awattar.get(config=config)
        except Exception as e:
            log.warning(
                "Error in Awattar data poller. Retrying in 15 seconds. {}".format(
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
            weather.get(config=config)
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
    """Entry point for the data poller."""
    loop = asyncio.get_event_loop()
    # Set pystore directory
    pystore.set_path(Path(config.file_location.data_dir).expanduser().as_posix())
    # Add all tasks to the event loop.
    task_match = re.compile(r"^.*_task$")
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
        log.info("Closing event loop")
        loop.close()


if __name__ == "__main__":
    config = read_config()
    start_logging(config)
    main(config)
