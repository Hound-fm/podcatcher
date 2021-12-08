# WARNING: Chainquery is unreliable.
# Some data is never updated or it takes a while.
# https://github.com/lbryio/chainquery/labels/type%3A%20bug

# TODO: Migrate to custom "Hub" and discard any external api usage.
# https://github.com/lbryio/hub

# Import dependencies
import time
import httpx
from logger import console
from utils import increase_delay_time, truncate_string
from config import config

# Global values
TIMEOUT_RETRY = 0
TIMEOUT_DELAY = config["TIMEOUT_DELAY"]
MAX_TIMEOUT_RETRY = config["MAX_TIMEOUT_RETRY"]

# Fix issues with chainquery api and pypika query format:
def formatQuery(q):
    return q.get_sql().replace('"', "")


# Default options for queries
default_query_options = {
    "limit": config["CHUNK_SIZE"],
    "offset": 0,
}

# Function to run a query and retrive data from the chainquery public api
def query(q, options=default_query_options, retry=0):
    try:
        # Apply options
        q = q.limit(options["limit"])
        q = q.offset(options["offset"])
        # Preapare string for url encoding
        queryString = formatQuery(q)
        # Send the sql query as url parameter
        payload = {"query": queryString}
        # Initial request
        res = httpx.get(config["CHAINQUERY_API"], params=payload, timeout=20.0)
        res.raise_for_status()
        # Parse response data to json
        res = res.json()
        data = res.get("data")
        # Retrive response data
        return data

    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        console.error(
            "CHAINQUERY",
            f"Error response {exc.response.status_code} while requesting {truncate_string(exc.request.url)!r}.",
        )
    # Handle timeout errors
    except httpx.TimeoutException as exc:
        global TIMEOUT_RETRY
        TIMEOUT_RETRY = retry + 1
        if TIMEOUT_RETRY < MAX_TIMEOUT_RETRY:
            console.warning("CHAINQUERY", exc)
            console.info("CHAINQUERY", "Retry query...")
            time.sleep(increase_delay_time(TIMEOUT_DELAY, TIMEOUT_RETRY))
            return query(q, options, TIMEOUT_RETRY)
        else:
            console.error(
                "CHAINQUERY",
                f"HTTP Exception for {truncate_string(exc.request.url)!r}",
                action=exc,
            )
    # Handle request errors
    except httpx.RequestError as exc:
        console.error(
            "CHAINQUERY",
            f"An error occurred while requesting {truncate_string(exc.request.url)!r}.",
            action=exec,
        )
