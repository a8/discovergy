# -*- coding: utf-8 -*-

"""

Discovergy API authentication module

Discovergy uses OAuth1. We use authlib.org to get auth tokens.

See https://docs.authlib.org/en/latest/client/oauth1.html#oauth-1-session and
https://api.discovergy.com/docs/
"""
from urllib.parse import urljoin, parse_qs
from typing import Any, Dict, List, NamedTuple, Union

import httpx

from box import Box  # type: ignore
from loguru import logger as log
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential  # type: ignore
from authlib.integrations.requests_client import OAuth1Session  # type: ignore

from .config import config_updater_factory, write_config_updater
from .defaults import API_URL, APP_NAME, API_HOST
from .utils import before_log


class OAuth1Token(NamedTuple):
    key: str
    client_secret: str
    token: str
    token_secret: str


@retry(
    before=before_log(log, "debug"),
    stop=(stop_after_delay(10) | stop_after_attempt(5)),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
def get_consumer_token(config: Box) -> Union[Dict[Any, Any], List[Any]]:
    """Return a new consumer token."""
    consumer_url = "/oauth1/consumer_token"
    consumer_token_url = urljoin(API_HOST, f"{API_URL}{consumer_url}")
    timeout = 10

    try:
        consumer_response = httpx.post(
            consumer_token_url, data={"client": APP_NAME}, timeout=timeout
        )
    except Exception as e:
        log.error(f"Caught an exception while fetching the consumer token: {e}")
        raise
    try:
        consumer_token = consumer_response.json()
    except Exception as e:
        log.error(f"Could not JSON decode the response for the consumer token: {e}")
        raise

    return consumer_token


@retry(
    before=before_log(log, "debug"),
    stop=(stop_after_delay(10) | stop_after_attempt(5)),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
def get_oath_verifier(config: Box, client: OAuth1Session) -> str:
    """Fetch a request token and return the oauth1 verifier token."""
    request_token_url = urljoin(API_HOST, f"{API_URL}/oauth1/request_token")
    authorize_url = urljoin(API_HOST, f"{API_URL}/oauth1/authorize")
    discovergy_email = config.discovergy_account.email
    discovergy_password = config.discovergy_account.password

    try:
        client.fetch_request_token(request_token_url)
    except Exception as e:
        log.warning(f"Caught exception while fetching the request token: {e}")
        raise
    authorize_url = client.create_authorization_url(
        authorize_url, email=discovergy_email, password=discovergy_password
    )
    try:
        verifier_response = httpx.get(authorize_url)
    except Exception as e:
        log.warning(f"Caught exception while GETing the authorize URL: {e}")
        raise
    verifier = parse_qs(verifier_response.text)
    try:
        oauth_verifier = verifier["oauth_verifier"].pop()
    except KeyError:
        log.warning("Could not get the oauth verifier. That might be fatal.")
        raise

    return oauth_verifier


@retry(
    before=before_log(log, "debug"),
    stop=(stop_after_delay(10) | stop_after_attempt(5)),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
def fetch_access_token(client: OAuth1Session, oauth_verifier: str) -> dict:
    """Fetch and return the OAuth1 access token."""
    access_token_url = urljoin(API_HOST, f"{API_URL}/oauth1/access_token")
    try:
        oauth_token = client.fetch_access_token(
            access_token_url, verifier=oauth_verifier
        )
    except Exception as e:
        log.warning(f"Failed to fetch the OAuth1 access token: {e}")
        raise

    return oauth_token


def fetch_new_oauth1_token(config: Box, save: bool = True) -> OAuth1Token:
    """Return a new OAuth1 token. Also, update the config."""

    consumer_token = get_consumer_token(config)
    log.debug(f"Fetched a new consumer token: {get_consumer_token.retry.statistics}")

    oauth_key, oauth_secret = (consumer_token[k] for k in ("key", "secret"))
    client = OAuth1Session(oauth_key, oauth_secret)
    oauth_verifier = get_oath_verifier(config, client)
    access_token = fetch_access_token(client, oauth_verifier)

    token = OAuth1Token(
        key=oauth_key,
        client_secret=oauth_secret,
        token=access_token["oauth_token"],
        token_secret=access_token["oauth_token_secret"],
    )
    config["oauth_token"] = token._asdict()
    if save:
        path, config_updater = config_updater_factory(config)
        write_config_updater(path, config_updater)

    return token


def get_oauth1_token(config: Box, save: bool = True) -> OAuth1Token:
    """Return a OAuth1Token from the config or fetch a new one if it is
    not in the config dict.
    """
    if "oauth_token" in config:
        return OAuth1Token(**config["oauth_token"])
    else:
        return fetch_new_oauth1_token(config, save=save)
