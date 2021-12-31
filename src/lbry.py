# WARNING: Low peformance and some litimations.
# TODO: Migrate to custom "Hub" and discard any external api usage.
# https://github.com/lbryio/hub

import json
import time
import httpx
import numpy as np
from logger import console
from config import config
from utils import unix_time_millis, increase_delay_time, truncate_string

# Global values
TIMEOUT_RETRY = 0
TIMEOUT_DELAY = config["TIMEOUT_DELAY"]
MAX_TIMEOUT_RETRY = config["MAX_TIMEOUT_RETRY"]


def api_get_request(url, url_params={}, payload={}):
    try:
        # Initial request test
        res = httpx.get(config["ODYSEE_API"] + url, params=url_params, timeout=20.0)
        res.raise_for_status()
        # Parse to json and return results
        res = res.json()
        data = res["data"]
        return data
    except httpx.RequestError as exc:
        console.error(
            "LBRY_API",
            f"An error occurred while requesting {truncate_string(exc.request.url)!r}.",
        )
    except httpx.HTTPStatusError as exc:
        console.error(
            "LBRY_API",
            f"Error response {exc.response.status_code} while requesting {truncate_string(exc.request.url)!r}.",
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
        res = httpx.post(
            config["LBRY_SDK_API"], headers=headers, json=payload, timeout=20.0
        ).json()
    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        console.error(
            "LBRY_SDK",
            f"Error response {exc.response.status_code} while requesting {truncate_string(exc.request.url)!r}.",
        )
    # Handle timeout errors
    except httpx.TimeoutException as exc:
        global TIMEOUT_RETRY
        TIMEOUT_RETRY = retry + 1
        if TIMEOUT_RETRY < MAX_TIMEOUT_RETRY:
            console.warning("LBRY_SDK", method, action=exc)
            console.info("LBRY_SDK", method, action="Retry...")
            time.sleep(increase_delay_time(TIMEOUT_DELAY, TIMEOUT_RETRY))
            return lbry_proxy(method, payload_data, TIMEOUT_RETRY)
        else:
            console.error(
                "LBRY_SDK",
                f"HTTP Exception for {truncate_string(exc.request.url)!r} - {exc}",
            )
    # Handle request errors
    except httpx.RequestError as exc:
        console.error(
            "LBRY_SDK",
            f"An error occurred while requesting {truncate_string(exc.request.url)!r}.",
        )

    return res


def api_post_request(url, payload={}):
    try:
        # Initial request test
        res = httpx.post(config["ODYSEE_API"] + url, data=payload, timeout=20.0)
        # Parse to json and return results
        return res.json()
    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        console.error(
            "LBRY_API",
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}.",
        )
    # Handle request errors
    except httpx.RequestError as exc:
        console.error(
            "LBRY_API", f"An error occurred while requesting {exc.request.url!r}."
        )


def get_view_count(claim_ids):
    try:
        data = {
            "auth_token": config["ODYSEE_API_TOKEN"],
            "claim_id": ",".join(claim_ids),
        }
        view_count = api_post_request("file/view_count", data)

        if view_count["success"] and len(view_count["data"]) > 0:
            return view_count["data"]
        else:
            return np.zeros(len(claim_ids))

    except NameError:
        console.error("LBRY_API", "Failed to retrive view counts")
        return np.zeros(len(claim_ids))


def get_likes_count(claim_ids):
    try:
        data = {
            "auth_token": config["ODYSEE_API_TOKEN"],
            "claim_ids": ",".join(claim_ids),
        }
        likes_count = []
        reaction_count = api_post_request("reaction/list", data)

        if reaction_count["success"]:
            reactions = reaction_count["data"]["others_reactions"]
            for reaction_id in claim_ids:
                reaction_data = reactions[reaction_id]
                likes_count.append(
                    reaction_data["like"] + reaction_data["investor_like"]
                )
        return likes_count

    except NameError:
        console.error("LBRY_API", "Failed to retrive view counts")
        return np.zeros(len(claim_ids))


# Get filtered list
def get_filtered_outpoints():
    try:
        blocked = api_get_request("file/list_blocked")["outpoints"]
        filtered = api_get_request("file/list_filtered")["outpoints"]
        return set([*blocked, *filtered])
    except NameError:
        console.error("LBRY_API", "Failed retrive blacklisted outpoints")


# Expose filtered list
filtered_outpoints = get_filtered_outpoints()
