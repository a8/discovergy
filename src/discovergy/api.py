# -*- coding: utf-8 -*-

"""

Discovergy api module

https://api.discovergy.com/docs/
"""
__author__ = "Frank Becker <fb@alien8.de>"
__copyright__ = "Frank Becker"
__license__ = "mit"

import json
import time

from pathlib import Path
from urllib.parse import urlencode, urljoin
from typing import Dict, List, Optional, Set, Union

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential  # type: ignore
from authlib.integrations.requests_client import OAuth1Session  # type: ignore

from .auth import get_oauth1_token
from .defaults import API_URL, API_HOST
from .utils import before_log, measure_duration


utc_now = time


class DiscovergyAPIError(Exception):
    """Generic API Error"""

    pass


class DiscovergyAPIQueryError(DiscovergyAPIError):
    """Error while querying the Discovergy API"""

    pass


class DiscovergyAPIClient:
    """Represents a Discovergy API Client."""

    session = None
    last_query_duration: Optional[float] = None

    def __init__(self, *, config: Box):
        """:param config: the internal config object"""
        self.config: Box = config

    def __repr__(self):
        return f"DiscovergyMeter:{self.meter_id}"

    @retry(
        before=before_log(log, "debug"),
        stop=(stop_after_delay(10) | stop_after_attempt(5)),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def _query(self, resource: str) -> dict:
        """Query the Discovergy API for the given url.

        The last query duration can be accessed as
        self.last_query_duration
        """
        if not self.session:
            log.debug("Initiating a new Disovergy API session.")
            self.session = get_new_api_session(self.config)
        url = urljoin(API_HOST, f"{API_URL}/{resource}")
        for cycle in range(2):
            log.debug(f"GETing {url} ...")
            try:
                with measure_duration() as measure:
                    request = self.session.get(url)
            except Exception as e:
                log.warning(f"Caught an exception while querying {url}: {e}")
                raise
            else:
                self.last_query_duration = measure.duration
            if request.status_code < 300:
                break
            elif request.status_code == 401:
                log.debug("Need to update the OAuth token.")
                self.config.pop("oauth_token")
            else:
                log.warning(
                    f"Got HTTP status code {request.status_code} while querying {url}. "
                    "Will re-try with a new OAuth token."
                )
                self.config.pop("oauth_token")
        else:
            log.error(f"Could not query {url}. HTTP status code: {request.status_code}")
            raise DiscovergyAPIQueryError(f"Could not query {url}.")
        try:
            data = request.json()
        except json.JSONDecodeError:
            log.error(
                f"Could not JSON decode Discovergy API response. Response body: {request.text}"
            )
            raise
        return data

    @staticmethod
    def gen_ms_timestamp(timestamp: Union[float, int]) -> int:
        """Return the given timestamp in ms as an integer.
        Discovergy uses this format.
        """
        discovergy_ts = str(timestamp).replace(".", "")
        discovergy_ts = (discovergy_ts + "000")[:13]
        return int(discovergy_ts)


class DiscovergyMeter(DiscovergyAPIClient):
    """Represents an energy meter.

    TODO: Implement load_profile, raw_load_profile
    """

    _field_names: Set[str] = set()
    reading_resolutions = {
        "raw": 86400,  # 1 day
        "three_minutes": 864000,  # 10 days
        "fifteen_minutes": 2678400,  # 31 days
        "one_hour": 8035200,  # 93 days
        "one_day": 315576000,  # 10 years
        "one_week": 631152000,  # 20 years
        "one_month": 1577880000,  # 50 years
        "one_year": 3155760000,  # 100 years
    }

    def __init__(self, *, meter: dict, config: Box):
        """Init meter given by the described meter.

        :param meter: the meter as described by Disovergy
        """
        super().__init__(config=config)
        if "meterId" not in meter:
            raise KeyError("The meter meta info must contain the key 'meterId'.")
        self.meter_id = meter["meterId"]
        self.metadata = meter
        self.config: Box = config

    @property
    def devices(self) -> dict:
        """Return the last reading."""
        params = urlencode({"meterId": self.meter_id})
        return self._query(f"devices?{params}")

    def _get_field_names(self) -> None:
        """Fetch the field names from the API and set self.field_names.

        There is no need to call this method. Call self.field_names instead.
        """
        params = urlencode({"meterId": self.meter_id})
        self._field_names = set(self._query(f"field_names?{params}"))

    def _validate_field_names(self, *, field_names: List[str]) -> bool:
        """Return True if all the given field names are supported. Otherwise,
        raise a ValueError."""
        if field_names and not self.field_names.issuperset(field_names):
            msg = (
                "At least some of the given field names {} are not "
                "the available field names {}".format(
                    ", ".join(field_names), ", ".join(self.field_names)
                )
            )
            log.error(msg)
            raise ValueError(msg)
        return True

    def _validate_timestamps(self, *, ts_from: int, ts_to: int) -> bool:
        """Return True if all the given field names are supported. Otherwise,
        raise a ValueError."""
        if ts_from >= ts_to:
            msg = (
                f"The from time {ts_from} must not be larger than the to time {ts_to}."
            )
            log.error(msg)
            ValueError(msg)
        return True

    @property
    def field_names(self) -> Set[str]:
        """Return the list of field names.
        To force re-fetching them from the API unset
        """
        if not self._field_names:
            self._get_field_names()
        return self._field_names

    def disaggregation(self, *, ts_from: int, ts_to: Optional[int] = None,) -> dict:
        """Return the disaggregation reading."""
        endpoint = "disaggregation?{}".format
        ts_from = self.gen_ms_timestamp(ts_from)
        params = {"meterId": self.meter_id, "from": ts_from}

        if ts_to:
            ts_to = self.gen_ms_timestamp(time.time())
            self._validate_timestamps(ts_from=ts_from, ts_to=ts_to)
            params["to"] = self.gen_ms_timestamp(ts_to)

        return self._query(endpoint(urlencode(params)))

    def activities(self, *, ts_from: int, ts_to: Optional[int] = None,) -> dict:
        """Return the activities reading.

        ts_to is required. If omitted it is set to now()."""
        endpoint = "activities?{}".format
        ts_from = self.gen_ms_timestamp(ts_from)
        params = {"meterId": self.meter_id, "from": ts_from}

        if ts_to:
            ts_to = self.gen_ms_timestamp(time.time())
            self._validate_timestamps(ts_from=ts_from, ts_to=ts_to)
            params["to"] = self.gen_ms_timestamp(ts_to)
        else:
            now = time.time()
            log.debug(
                f"Auto-adding timestamp {now} as 'to' parameter. It is "
                "required in the 'activities' query."
            )
            ts_to = self.gen_ms_timestamp(now)
        params["to"] = ts_to

        return self._query(endpoint(urlencode(params)))

    def last_reading(self) -> dict:
        """Return the last reading."""
        params = urlencode({"meterId": self.meter_id})
        return self._query(f"last_reading?{params}")

    def readings(
        self,
        *,
        disaggregation: Optional[bool] = None,
        field_names: Optional[List[str]] = None,
        ts_from: int,
        ts_to: Optional[int] = None,
        resolution: Optional[str] = None,
    ) -> List[Dict]:
        """Return the last reading."""
        endpoint = "readings?{}".format
        ts_from = self.gen_ms_timestamp(ts_from)
        params = {"meterId": self.meter_id, "from": ts_from}
        if ts_to:
            ts_to = self.gen_ms_timestamp(time.time())
            self._validate_timestamps(ts_from=ts_from, ts_to=ts_to)
            params["to"] = self.gen_ms_timestamp(ts_to)
        if field_names and self._validate_field_names(field_names=field_names):
            params["fields"] = ",".join(field_names)
        if resolution and resolution not in self.reading_resolutions:
            msg = "The resolution argument {} is not known as one of " "{}.".format(
                resolution, ", ".join(self.reading_resolutions.keys())
            )
            log.error(msg)
            raise ValueError(msg)
        elif resolution:
            params["resolution"] = resolution
        if disaggregation and resolution and resolution != "raw":
            msg = (
                "If disaggregation is set the argument 'resolution' must be set to 'raw' "
                f"and not to '{resolution}'."
            )
            log.error(msg)
            raise ValueError(msg)
        elif disaggregation:
            params["disaggregation"] = "true"

        return self._query(endpoint(urlencode(params)))

    def statistics(
        self,
        *,
        field_names: Optional[List[str]] = None,
        ts_from: int,
        ts_to: Optional[int] = None,
    ) -> dict:
        """Return various statistics calculated over all measurements for the specified meter in the specified time interval."""
        endpoint = "statistics?{}".format
        ts_from = self.gen_ms_timestamp(ts_from)
        params = {"meterId": self.meter_id, "from": ts_from}

        if field_names and self._validate_field_names(field_names=field_names):
            params["fields"] = ",".join(field_names)
        if ts_to:
            ts_to = self.gen_ms_timestamp(time.time())
            self._validate_timestamps(ts_from=ts_from, ts_to=ts_to)
            params["to"] = self.gen_ms_timestamp(ts_to)
        else:
            now = time.time()
            log.debug(
                f"Auto-adding timestamp {now} as 'to' parameter. It is "
                "required in the 'statistics' query."
            )
            ts_to = self.gen_ms_timestamp(now)
        params["to"] = ts_to

        return self._query(endpoint(urlencode(params)))


def get_new_api_session(config: Box):
    """Return an authenticated session to the Discovergy API."""
    if "oauth_token" in config:
        token = config["oauth_token"]
    else:
        get_oauth1_token(config)
        token = config["oauth_token"]

    # Construct OAuth session with access token
    discovergy_oauth_session = OAuth1Session(
        token["key"],
        client_secret=token["client_secret"],
        token=token["token"],
        token_secret=token["token_secret"],
    )

    return discovergy_oauth_session


def describe_meters(config: Box) -> dict:
    """Describe and return all the meters for the given account."""
    for cycle in range(2):
        session = get_new_api_session(config)
        try:
            request = session.get(urljoin(API_HOST, f"{API_URL}/meters"))
        except Exception as e:
            log.warning(f"Caught an exception while describing all meters: {e}")
            raise
        if request.status_code < 300:
            break
        elif request.status_code == 401:
            log.debug("Need to update the OAuth token.")
            config.pop("oauth_token")
        else:
            log.warning(
                f"Got HTTP status code {request.status_code} while describing the meters. "
                "Will re-try with a new OAuth token."
            )
            config.pop("oauth_token")
    else:
        log.error(
            f"Could not describe the meters. HTTP status code: {request.status_code}"
        )
        raise DiscovergyAPIError("Could not describe the meters.")
    log.debug(f"Described meters in {request.elapsed}.")
    try:
        meters = request.json()
    except json.JSONDecodeError:
        log.error(
            f"Could not JSON decode described meters. Response body: {request.text}"
        )
        raise
    return meters


def save_meters(*, config: Box, meters: Dict) -> None:
    """Store the meters metadata in the config directory"""
    # meter_ids = glom(meters, ["meterId"])
    file_path = config.config_file_path.parent / Path("meters-metadata.json")
    to_save = meters
    missing = []
    try:
        with file_path.open() as fh:
            old_metadata = json.load(fh)
    except FileNotFoundError:
        log.debug(f"Did not find existing {file_path}.")
        old_metadata = None
    except json.decoder.JSONDecodeError:
        log.debug(
            f"Could not JSON decode the content of {file_path}. Will overwrite that file."
        )
        old_metadata = None
    else:
        # Let's check what has changed.
        for old_meter_id, old_meter in old_metadata.items():
            if old_meter_id in meters:
                if old_meter != meters[old_meter_id]:
                    log.debug(f"Updating the metadata on disk for {old_meter_id}.")
            else:
                # keep old meters
                missing.append(old_meter_id)
                to_save[old_meter_id] = old_meter
        if missing:
            log.warning(
                "Previously described the meters{}: {}".format(
                    ["s", ""][len(missing) < 1], ", ".join(missing)
                )
            )
    try:
        with file_path.open("w") as fh:
            json.dump(to_save, fh)
    except FileNotFoundError:
        log.error(f"Could not save meters metadata to {file_path}.")
        raise
