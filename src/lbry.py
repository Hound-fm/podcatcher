# WARNING: Low peformance and some litimations.
# TODO: Migrate to custom "Hub" and discard any external api usage.
# https://github.com/lbryio/hub

import json
import time
import httpx
import numpy as np
from logger import log
from config import config
from utils import unix_time_millis, increase_delay_time, truncate_string
from constants import LBRY_API, LBRY_TOKEN, LBRY_COM_API

# Global values
TIMEOUT_RETRY = 0
TIMEOUT_DELAY = config["TIMEOUT_DELAY"]
MAX_TIMEOUT_RETRY = config["MAX_TIMEOUT_RETRY"]


def api_get_request(url, url_params={}, payload={}):
    try:
        # Initial request test
        res = httpx.get(LBRY_COM_API + url, params=url_params)
        res.raise_for_status()
        # Parse to json and return results
        res = res.json()
        data = res["data"]
        return data
    except httpx.RequestError as exc:
        log.error(
            f"An error occurred while requesting {truncate_string(exc.request.url)!r}."
        )
    except httpx.HTTPStatusError as exc:
        log.error(
            f"Error response {exc.response.status_code} while requesting {truncate_string(exc.request.url)!r}."
        )


def lbry_proxy(method, payload_data, retry=0):
    res = None
    headers = {"Content-Type": "application/json-rpc", "user-agent": "hound.fm/0.0.1"}
    payload = {
        "id": unix_time_millis(),
        "jsonrpc": "2.0",
        "method": method,
        "params": payload_data,
    }
    # Initial request test
    try:
        res = httpx.post(LBRY_API, headers=headers, json=payload).json()
    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        log.error(
            f"Error response {exc.response.status_code} while requesting {truncate_string(exc.request.url)!r}."
        )
    # Handle timeout errors
    except httpx.TimeoutException as exc:
        global TIMEOUT_RETRY
        TIMEOUT_RETRY = retry + 1
        if TIMEOUT_RETRY < MAX_TIMEOUT_RETRY:
            log.warning(f"LBRY_PROXY: {method} - {exc}")
            log.info(f"LBRY_PROXY: {method} - retry...")
            time.sleep(increase_delay_time(TIMEOUT_DELAY, TIMEOUT_RETRY))
            return lbry_proxy(method, payload_data, TIMEOUT_RETRY)
        else:
            log.error(
                f"HTTP Exception for {truncate_string(exc.request.url)!r} - {exc}"
            )
    # Handle request errors
    except httpx.RequestError as exc:
        log.error(
            f"An error occurred while requesting {truncate_string(exc.request.url)!r}."
        )

    return res


def api_post_request(url, payload={}):
    try:
        # Initial request test
        res = httpx.post(LBRY_COM_API + url, data=payload)
        # Parse to json and return results
        return res.json()
    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        log.error(
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}."
        )
    # Handle request errors
    except httpx.RequestError as exc:
        log.error(f"An error occurred while requesting {exc.request.url!r}.")


def get_view_counts(claim_ids):
    try:
        data = {"auth_token": LBRY_TOKEN, "claim_id": ",".join(claim_ids)}
        view_counts = api_post_request("file/view_count", data)
        return view_counts["data"]

    except NameError:
        log.error("Failed to retrive view counts")
        return np.zeros(len(claim_ids))


# Get filtered list
def get_filtered_outpoints():
    try:
        blocked = api_get_request("file/list_blocked")["outpoints"]
        filtered = api_get_request("file/list_filtered")["outpoints"]
        return set([*blocked, *filtered])
    except NameError:
        log.error("Failed retrive blacklisted outpoints")


# Expose filtered list
filtered_outpoints = get_filtered_outpoints()
