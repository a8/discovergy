# -*- coding: utf-8 -*-

"""Discovergy weather module

Pull weather data.
"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import sys

from pathlib import Path

import arrow  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log
from pyowm import OWM  # type: ignore

from .utils import write_data


def get_open_weather_map(*, config: Box) -> None:
    """Fetch and write the Open Weather Map data."""
    try:
        owm_id = config.open_weather_map["id"]
        latitude = float(config.open_weather_map["latitude"])
        longitude = float(config.open_weather_map["longitude"])
    except KeyError:
        log.error(
            "The config file does not contain all Open Weather Map config keys (id, latitude, longitude). Cannot continue."
        )
        sys.exit(1)
    if owm_id.lower() == "none":
        log.debug("Open Weather Map is not configured.")
        return

    open_weather_map = OWM(owm_id)
    if not open_weather_map.is_API_online():
        log.warning("Open Weather Map endpoint is not online-line.\n")
        return
    start_ts = arrow.utcnow()
    try:
        weather = open_weather_map.weather_at_coords(latitude, longitude)
    except Exception as e:
        log.warning("Could not fetch weather: {}.".format(str(e)))
        return
    else:
        elapsed_time = arrow.utcnow() - start_ts
        log.debug(f"Fetching Open Weather Map took {elapsed_time}.")

    date = arrow.utcnow()
    file_name = f"open_weather_map_{date.format('YYYY-MM-DD_HH-mm-ss')}.json.gz"
    file_path = Path(config.file_location.data_dir) / Path(file_name)
    write_data(data=weather.to_JSON(), file_path=file_path)
