# WARNING: Chainquery is unreliable.
# Some data is never updated or it takes a while.
# https://github.com/lbryio/chainquery/labels/type%3A%20bug

# TODO: Migrate to custom "Hub" and discard any external api usage.
# https://github.com/lbryio/hub

# Import dependencies
import httpx
from constants import CHAINQUERY_API

# Fix issues with chainquery api and pypika query format:
def formatQuery(q):
    return q.get_sql().replace('"', "")


# Default options for queries
default_query_options = {"limit": 100, "offset": 0}

# Function to run a query and retrive data from the chainquery public api
def query(q, options=default_query_options):
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

    # Handle request errors
    except httpx.RequestError as exc:
        print(exc)
        print(f"An error occurred while requesting {exc.request.url!r}.")

    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        print(
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}."
        )
