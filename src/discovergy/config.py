# -*- coding: utf-8 -*-

"""

Discovergy Config

"""

import copy
import getpass
import os
import sys

from pathlib import Path
from typing import Optional

import schema

from box import Box
from configupdater import ConfigUpdater
from loguru import logger as log

from .defaults import DEFAULT_CONFIG
from .utils import verify_file_permissions


def bootstrap_config(path: Optional[str] = None) -> ConfigUpdater:
    """Create the Config file and populate it."""
    if path is None:
        path = Path(os.path.expanduser("~")) / ".config" / "discovergy" / "config.ini"
    if not path.parent.is_dir():
        os.makedirs(path.parent.as_posix())
    config_updater = ConfigUpdater()
    config_updater.read_string(DEFAULT_CONFIG)
    discovergy_account_email = input(
        "Please tell me your Discovergy account user e-mail: "
    )
    discovergy_account_password = getpass.getpass(
        "Please tell me your Discovergy account password: "
    )
    save_account_password = False
    while True:
        save_account_password = input(
            "Do you want to save the password to the config file? It will be required to fetch a new "
            "Oauth token once the current one expires. (y/n)"
        )
        if save_account_password.lower() == "y":
            save_account_password = True
            break
        elif save_account_password.lower() == "n":
            save_account_password = False
            break
    open_weather_map = input("Please enter the Open Weather Map id. <Enter> for none.")

    config_updater.set("discovergy_account", "email", value=discovergy_account_email)
    config_updater.set(
        "discovergy_account", "password", value=discovergy_account_password
    )
    config_updater.set(
        "discovergy_account", "save_password", value=save_account_password
    )
    config_updater.set("open_weather_map", "id", value=open_weather_map)
    write_config_updater(path, config_updater)
    return config_updater


def write_config_updater(path: Path, config: ConfigUpdater) -> None:
    """Write the config file."""
    to_write_config = copy.deepcopy(config)
    # Do not save the pwd if that's not wanted!
    if (
        config.has_option("discovergy_account", "save_password")
        and not config.get("discovergy_account", "save_password").value
    ):
        to_write_config.set("discovergy_account", "password", value="")
    with os.fdopen(
        os.open(path.as_posix(), os.O_WRONLY | os.O_CREAT, 0o600), "w"
    ) as fh:
        to_write_config.write(fh)


def verify_config(config: Box) -> bool:
    """Return (True|False) result if the config matches the schema."""
    config_schema = schema.Schema(
        {
            "discovergy_account": {
                "email": schema.And(str, len),
                "password": schema.Optional(str),
                "save_password": schema.And(
                    schema.Use(str.lower), lambda x: x in ("true", "false")
                ),
            },
            "auth_token": {"token": str},
            "file_location": {"data_dir": str, "log_dir": str,},
            "poll": {"default": schema.Use(int), "try_sleep": schema.Use(int),},
            schema.Optional("open_weather_map"): {"id": str},
        }
    )
    try:
        config_schema.validate(config)
    except schema.SchemaError as e:
        log.error(f"Caught a config schema violation error: {e}")
        return False
    return True


def read_config(path: Optional[str] = None) -> Box:
    """Return the config"""
    config = Box(box_it_up=True)
    if path:
        config_path_locations = (path,)
    else:
        config_path_locations = (
            Path(Path("/etc") / "discovergy" / "config.ini"),
            Path(os.path.expanduser("~")) / ".config" / "discovergy" / "config.ini",
        )
    found_config_file = False
    for path in config_path_locations:
        if path.exists():
            found_config_file = True
            break
    else:
        log.info(f"No config file found in {path.parent}. Creating one...")
        path = Path(os.path.expanduser("~")) / ".config" / "discovergy" / "config.ini"
        config_updater = bootstrap_config(path)
    if path.parent.exists() and not path.parent.is_dir():
        log.error(f"Expected the config directory {path.parent} to be a directory.")
        sys.exit(1)

    if not verify_file_permissions(path):
        log.error(
            f"Could not ensure secure file permissions for {path}. Fix them and try again."
        )
        sys.exit(1)

    if found_config_file:
        config_updater = ConfigUpdater()
        try:
            config_updater.read(path.as_posix())
        except Exception as e:
            log.error(f"Could not read the config from {path}: {e}")
            sys.exit(1)

    config = Box(config_updater.to_dict(), box_dots=True)

    # Strip off quotes that made it into the config.ini file
    config.file_location.data_dir = config.file_location.data_dir.strip("\"'")
    config.file_location.log_dir = config.file_location.log_dir.strip("\"'")

    return config
