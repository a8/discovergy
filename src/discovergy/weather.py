# -*- coding: utf-8 -*-

"""Discovergy weather module

Pull weather data.
"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import json
import sys

from typing import Dict, Optional

import arrow  # type: ignore
import numpy as np  # type: ignore
import pandas as pd  # type: ignore

from box import Box  # type: ignore
from loguru import logger as log
from pyowm import OWM  # type: ignore

from .utils import write_data_frames


def get(*, config: Box) -> None:
    """Fetch and write weather data.

    Note, for now only Open Weather Map is supported. Once multiple
    weather data sources are configurable this is going to be a dispatcher."""

    owm_data = get_open_weather_map(config=config)
    if not owm_data:
        return

    df = raw_owm_to_df(data=owm_data)
    write_data_frames(config=config, data_frames=[df], name="weather")


def get_open_weather_map(*, config: Box) -> Optional[Dict]:
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
        log.warning("Open Weather Map endpoint is not online-line.")
        return
    start_ts = arrow.utcnow()
    try:
        weather = open_weather_map.weather_at_coords(latitude, longitude)
    except Exception as e:
        log.warning("Could not fetch weather: {}.".format(str(e)))
        return
    else:
        elapsed_time = arrow.utcnow() - start_ts
        log.debug(
            f"Fetching Open Weather Map took {elapsed_time.total_seconds():.3f} s."
        )
    try:
        weather = json.loads(weather.to_JSON())
    except json.JSONDecodeError as e:
        log.warning(f"Could not JSON decode weather data: {e}.")
    except Exception as e:
        log.warning(f"Could not convert weather data {weather} to JSON: {e}.")
        return
    return weather


def raw_owm_to_df(*, data: Dict) -> pd.DataFrame:
    """Return the raw OWM Weather data as a Pandas DataFrame."""
    # 1) Only store what we don't know. Dropping location info.
    weather_data = data["Weather"]
    # 2) Flatten nested data
    temperature_data = weather_data.pop("temperature")
    for k, v in temperature_data.items():
        weather_data[k] = v
    pressure_data = weather_data.pop("pressure")
    weather_data["pressure"] = pressure_data["press"]
    weather_data["sea_level"] = pressure_data["sea_level"]
    wind_data = weather_data.pop("wind")
    weather_data["wind_speed"] = wind_data["speed"]
    weather_data["wind_direction"] = wind_data["deg"]
    rain_data = weather_data.pop("rain")
    for k, v in rain_data.items():
        weather_data[f"rain_{k}"] = v
    snow_data = weather_data.pop("snow")
    for k, v in snow_data.items():
        weather_data[f"snow_{k}"] = v
    # 3) Get index
    time_stamp = weather_data.pop("reference_time")
    index = [pd.Timestamp(time_stamp, unit="s", tz="utc")]
    # 4) Gen data frame
    for k, v in weather_data.items():
        if v is None:
            weather_data[k] = np.nan
    df = pd.DataFrame(weather_data, index=index)

    return df


def data_from_files():
    from .config import read_config
    import os
    import json
    config = read_config()
    for f in os.listdir():
        if f.startswith("open_weather_map"):
            print(f)
            with open(f) as fh:
                data = json.loads(json.load(fh))
            df = raw_owm_to_df(data=data)
            write_data_frames(
                config=config,
                data_frames=[df],
                name="weather"
            )
