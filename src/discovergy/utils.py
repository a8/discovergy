# -*- coding: utf-8 -*-

"""

Discovergy shared helper code

"""
import os
import re
import sys

from pathlib import Path
from typing import Any, NamedTuple, Tuple, Union

from box import Box
from loguru import logger as log


TimeStampedValue = NamedTuple('TimeStampedValue', [('timestamp', float), ('value', Any)])
ValueUnit = NamedTuple('ValueUnit', [('value', Union[float, int]), ('unit', str)])


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
                "rotation": "1 week",
                "compression": "gz",
                "format": "{time:YYYY-MM-DDTHH:mm:ss} | {level} | {message}",
                "serialize": True,
            },
        ],
        "extra": {"user": "someone"},
    }
    log.configure(**log_config)


def str2bool(value: str) -> bool:
    """Return the boolean value of the value given as a str."""
    if value.lower() in ['true', '1', 't', 'y', 'yes', 'yeah']:
        return True

    return False


def humanize_timediff(timediff: int) -> Tuple[int, int, int, int]:
    """Return (days, hours, minutes, seconds) of timespan in seconds"""
    minutes, seconds = divmod(timediff, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    return days, hours, minutes, seconds


def get_humanized_timediff_str(timediff: int) -> str:
    """Return a human readable string of the given timediff. The length is
    dynamic.

    Examples:

    >>> get_humanize_timediff_str(0)
    '0s'

    >>> get_humanize_timediff_str(1)
    '1s'

    >>> get_humanize_timediff_str(10)
    '10s'

    >>> get_humanize_timediff_str(60)
    '01:00'

    >>> get_humanize_timediff_str(23 * 3600 + 1)
    '23:00:01'

    >>> get_humanize_timediff_str(24 * 3600 + 1)
    '1T00:00:01'

    """
    if timediff < 1:
        return "{0:.3f}s".format(timediff)
    if timediff == 0:
        return '0s'
    elements = [int(e) for e in humanize_timediff(timediff)]
    # Reduce the list to only values
    count_0 = 0
    for e in elements:
        if not e:
            count_0 += 1
        else:
            break
    elements = elements[count_0:]
    time_fs = ":".join(len(elements) * ['{:02d}'])
    if len(elements) == 4:
        time_fs = time_fs.replace('{:02d}:', '{:d}T', 1)
    elif len(elements) == 1:
        time_fs = '{:d}s'

    return time_fs.format(*elements)


def verify_file_permissions(path: Path) -> bool:
    """Return (True|False) if the file system access rights are set to current user only."""
    if path.is_file:
        file_stat = path.stat()
        if file_stat.st_uid != os.getuid():
            return False

        if re.match(r'0o*100[0-6]00', oct(file_stat.st_mode)):
            return True
        try:
            os.chmod(path, 0o600)
        except OSError:
            log.error(f"Tried to change the permissions of {path} but failed. "
                      "Please fix the permissions to max. 0600 yourself!")
            return False
        else:
            log.warning("The file {} didn't have secure file permissions {}. "
                        "The permissions were changed to -rw------- for you. ".format(
                            path, oct(file_stat.st_mode)))
            return True
    return False
