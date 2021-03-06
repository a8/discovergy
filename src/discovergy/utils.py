# -*- coding: utf-8 -*-

"""

Discovergy shared helper code

"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import gzip
import json
import os
import re
import sys

from contextlib import ContextDecorator
from pathlib import Path
from timeit import default_timer
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Union

import pandas as pd  # type: ignore
import pystore

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import _utils  # type: ignore


class TimeStampedValue(NamedTuple):
    timestamp: float
    value: Any


class ValueUnit(NamedTuple):
    value: Union[float, int]
    unit: str


class measure_duration(ContextDecorator):
    """A context manager that measures time from enter to exit."""

    def __enter__(self):
        self.start = default_timer()
        return self

    def __exit__(self, *exc):
        self.duration = default_timer() - self.start
        return False


def start_logging(config: Box) -> None:
    """Start console and file logging"""
    log_dir = Path(config.file_location.log_dir).expanduser()
    if not log_dir.is_dir():
        sys.stderr.write(f"Could not find the log dir {log_dir}. Creating it ...\n")
        os.makedirs(log_dir.as_posix())
    log_config = {
        "handlers": [
            {
                "sink": sys.stderr,
                "format": "{time:YYYY-MM-DD HH:mm:ss} | <level>{level}</level> | {message}",
                "colorize": True,
                "level": "DEBUG",
                "backtrace": True,
            },
            {
                "sink": log_dir / "discovergy_{time}.log",
                "rotation": "1 day",
                "compression": "gz",
                "format": "{time:YYYY-MM-DDTHH:mm:ss} | {level} | {message}",
                "backtrace": True,
                "serialize": False,
            },
        ],
        "extra": {"user": "someone"},
    }
    log.configure(**log_config)  # type: ignore


def before_log(logger: Any, log_level: str) -> Callable:
    """Before call strategy that logs to some logger the attempt."""

    def log_it(retry_state):
        logger = getattr(log, log_level)
        logger(
            f"Starting call to '{_utils.get_callback_name(retry_state.fn)}', "
            f"this is the {_utils.to_ordinal(retry_state.attempt_number)} time calling it."
        )

    return log_it


def split_df_by_month(*, df) -> List[pd.DataFrame]:
    """Return data frames split by month."""
    data_frames = []
    intervals = sorted(set([(e.year, e.month) for e in df.index.unique()]))
    if len(intervals) == 1:
        # One month only, early return
        data_frames.append(df)
        return data_frames
    date_range = pd.date_range(
        "{}-{:02d}".format(intervals[0][0], intervals[0][1]),
        periods=len(intervals),
        freq="M",
        tz="UTC",
    )
    prev_month = date_range[0]
    data_frames.append(df[df.index <= date_range[0]])
    for date in date_range[1:]:
        df_per_month = df[(prev_month < df.index) & (df.index <= date)]
        data_frames.append(df_per_month)
        prev_month = date
    return data_frames


def split_df_by_day(*, df) -> List[pd.DataFrame]:
    """Return data frames split by day."""
    data_frames = []
    intervals = sorted(set([(e.year, e.month, e.day) for e in df.index.unique()]))
    if len(intervals) == 1:
        # One day only, early return
        data_frames.append(df)
        return data_frames
    date_range = pd.date_range(
        "{}-{:02d}-{:02d}".format(intervals[0][0], intervals[0][1], intervals[0][2]),
        periods=len(intervals),
        freq="D",
        # tz="UTC",
    )
    # date_range starts at 0h 00m 00s
    prev_day = date_range[0]
    for date in date_range[1:]:
        df_per_day = df[(prev_day < df.index) & (df.index <= date)]
        data_frames.append(df_per_day)
        prev_day = date
    return data_frames


def str2bool(value: str) -> bool:
    """Return the boolean value of the value given as a str."""
    if value.lower() in ["true", "1", "t", "y", "yes", "yeah"]:
        return True

    return False


def verify_file_permissions(path: Path) -> bool:
    """Return (True|False) if the file system access rights are set to current user only."""
    if path.is_file:
        file_stat = path.stat()
        if file_stat.st_uid != os.getuid():
            return False

        if re.match(r"0o*100[0-6]00", oct(file_stat.st_mode)):
            return True
        try:
            os.chmod(path, 0o600)
        except OSError:
            log.error(
                f"Tried to change the permissions of {path} but failed. "
                "Please fix the permissions to max. 0600 yourself!"
            )
            return False
        else:
            log.warning(
                "The file {} didn't have secure file permissions {}. "
                "The permissions were changed to -rw------- for you. ".format(
                    path, oct(file_stat.st_mode)
                )
            )
            return True
    return False


def write_data(*, data: List[Dict], file_path: Path) -> None:
    """Write the gz-iped raw data to file_path."""
    dst_dir = file_path.parent
    if not dst_dir.expanduser().is_dir():
        log.warning(f"Creating the data destination directory {dst_dir}.")
        os.makedirs(dst_dir.expanduser().as_posix())

    with gzip.open(file_path.expanduser().as_posix(), "wb") as fh:
        fh.write(json.dumps(data).encode("utf-8"))


def write_data_frames(
    *, config: Box, data_frames: List[pd.DataFrame], name: str
) -> None:
    """Create or update the data as a Pandas DataFrame in hdf5 file."""
    if not data_frames:
        log.debug(f"Did not receive any data for {name}.")
        return
    for df in data_frames:
        if not len(df):
            log.debug(f"Did not find any data in {df}. Skipping...")
            continue
        first_ts = min(df.index)
        file_name = f"{name}_{first_ts.year}-{first_ts.month:02d}.hdf5"
        file_path = Path(config.file_location.data_dir) / Path(file_name)
        file_path = file_path.expanduser()
        if file_path.is_file():
            df_prev = pd.read_hdf(file_path, name)
            df = df.combine_first(df_prev)
        df.to_hdf(file_path, key=name)


def write_data_to_pystore(
    *,
    config: Box,
    data_frames: List[pd.DataFrame],
    name: str,
    metadata: Optional[Dict] = None,
) -> None:
    """Create or update the pandas.DataFrames as Pystore collection.items.

    The DataFrames must contain time series data with the index of type datetime64.
    The lowest index (min(index)) will be converted as YYYY-MM string and set
    as the item name.
    Each dataframe must only contain data of one day! This function doesn't check max(df.index).

    Note, PyStore will make sure there is a unique index:

    ~/.../site-packages/pystore/collection.py in append(self, item, data, npartitions, epochdate, threaded, reload_items, **kwargs)
    183         # combined = current.data.append(new)
    184         combined = dd.concat([current.data, new]).drop_duplicates(keep="last")

    PyStore:
    https://medium.com/@aroussi/fast-data-store-for-pandas-time-series-data-using-pystore-89d9caeef4e2
    """

    if metadata is None:
        metadata = {}
    if not data_frames:
        log.debug(f"Did not receive any data for {name}.")
        return
    store = pystore.store("discovergy")
    collection = store.collection(name)
    item_names = collection.list_items()
    for df in data_frames:
        if not len(df):
            log.debug(f"Did not find any data in {df}. Skipping...")
            continue
        first_ts = min(df.index)
        item_name = f"{first_ts.year}-{first_ts.month:02d}"
        if item_name in item_names:
            # FIXME (a8): Create one partition per day. There must be a better way. Issue is that
            # pandas loads the full pd.DataFrame into memory. That requires memory.
            npartitions = first_ts.day
            log.debug(f"Appended to {item_name} {first_ts}.")
            collection.append(item_name, df, npartitions=npartitions)
        else:
            log.debug("Created new Dask DF.")
            collection.write(item_name, df, metadata=metadata, overwrite=False)
