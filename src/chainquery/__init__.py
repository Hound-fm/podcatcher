# WARNING: Chainquery is unreliable.
# Some data is never updated or it takes a while.
# https://github.com/lbryio/chainquery/labels/type%3A%20bug

# TODO: Migrate to custom "Hub" and discard any external api usage.
# https://github.com/lbryio/hub

# Import dependencies
import time
import httpx
from constants import CHAINQUERY_API
from logger import log
from utils import increase_delay_time
from analysis.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_TIMOUT_DELAY,
)

# Global values
TIMEOUT_RETRY = 0
TIMEOUT_DELAY = DEFAULT_TIMOUT_DELAY
MAX_TIMEOUT_RETRY = 5

# Fix issues with chainquery api and pypika query format:
def formatQuery(q):
    return q.get_sql().replace('"', "")


# Default options for queries
default_query_options = {
    "limit": DEFAULT_CHUNK_SIZE,
    "offset": 0,
}

# Function to run a query and retrive data from the chainquery public api
def query(q, options=default_query_options, retry=0):
    # Query response
    res = None
    try:
        # Apply options
        q = q.limit(options["limit"])
        q = q.offset(options["offset"])
        # Preapare string for url encoding
        queryString = formatQuery(q)
        # Send the sql query as url parameter
        payload = {"query": queryString}
        # Initial request
        res = httpx.get(CHAINQUERY_API, params=payload)
        res.raise_for_status()
        # Parse response data to json
        res = res.json()
        data = res["data"]
        # Retrive response data
        return data

    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        log.error(
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}."
        )
    # Handle timeout errors
    except httpx.TimeoutException as exc:
        global TIMEOUT_RETRY
        TIMEOUT_RETRY = retry + 1
        if TIMEOUT_RETRY < MAX_TIMEOUT_RETRY:
            log.warning(f"Chainquery: {exc}")
            log.info(f"Chainquery: retry...")
            time.sleep(increase_delay_time(TIMEOUT_DELAY, TIMEOUT_RETRY))
            return query(q, options, TIMEOUT_RETRY)
        else:
            log.error(f"HTTP Exception for {exc.request.url} - {exc}")
    # Handle request errors
    except httpx.RequestError as exc:
        log.error(f"An error occurred while requesting {exc.request.url!r}.")
