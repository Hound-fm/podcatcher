import json
import time
import httpx
import numpy as np
from utils import unix_time_millis
from constants import LBRY_API, LBRY_TOKEN, LBRY_COM_API

# Global values
TIMEOUT_RETRY = 0
TIMEOUT_DELAY = 24
MAX_TIMEOUT_RETRY = 3


def api_get_request(url, url_params={}, payload={}):
    try:
        # Initial request test
        res = httpx.get(LBRY_COM_API + url, params=url_params)
        res.raise_for_status()
        # Parse to json and return results
        res = res.json()
        data = res["data"]
        return data
    except NameError:
        print(NameError)


def lbry_proxy(method, payload_data, retry=0):
    global TIMEOUT_RETRY
    TIMEOUT_RETRY = retry + 1
    res = None
    headers = {"Content-Type": "application/json-rpc", "user-agent": "my-app/0.0.1"}
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
        print(
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}."
        )
    # Handle timeout errors
    except httpx.TimeoutException as exc:
        print(f"HTTP Exception for {exc.request.url} - {exc}")
        time.sleep(TIMEOUT_DELAY)
        if TIMEOUT_RETRY < MAX_TIMEOUT_RETRY:
            return lbry_proxy(method, payload_data, TIMEOUT_RETRY)
    # Handle request errors
    except httpx.RequestError as exc:
        print(f"An error occurred while requesting {exc.request.url!r}.")

    return res


def api_post_request(url, payload={}):
    try:
        # Initial request test
        res = req.post(LBRY_COM_API + url, data=payload)
        res.raise_for_status()
        # Parse to json and return results
        return res.json()
    except NameError:
        print(NameError)


# Get filtered list
def get_filtered_outpoints():
    try:
        blocked = api_get_request("file/list_blocked")["outpoints"]
        filtered = api_get_request("file/list_filtered")["outpoints"]
        return set([*blocked, *filtered])
    except NameError:
        print("failed retrive blacklisted outpoints:", NameError)


# Expose filtered list
filtered_outpoints = get_filtered_outpoints()


def get_view_counts(claim_ids):
    try:
        data = {"auth_token": LBRY_TOKEN, "claim_id": ",".join(claim_ids)}
        view_counts = api_post_request("file/view_count", data)
        return view_counts["data"]

    except NameError:
        print(NameError)
        return np.zeros(len(claim_ids))
