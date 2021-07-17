# WARNING: Chainquery is unreliable.
# Some data is never updated or it takes a while.
# https://github.com/lbryio/chainquery/labels/type%3A%20bug

# TODO: Migrate to custom "Hub" and discard any external api usage.
# https://github.com/lbryio/hub

# Import dependencies
import httpx
from pypika import Query, Order, Tables, Criterion, functions as fn
from constants import CLAIM_TYPE, CONTENT_TYPE_AUDIO, CHAINQUERY_API

# Blocked channels list
BLOCKED_CHANNELS = {}

# Tables definition
tag, claim, claim_tag, support = Tables("TAG", "CLAIM", "CLAIM_TAG", "SUPPORT")

# Filter all non-audio content
def filter_by_content_type(content_type=CONTENT_TYPE_AUDIO):
    return Criterion.all(
        [
            claim.type == CLAIM_TYPE["STREAM"],
            claim.content_type.like(content_type + "%"),
        ]
    )

# Filter all content by audio duration:
def filter_by_audio_duration(min=0):
    return Criterion.all([claim.audio_duration.notnull(), claim.audio_duration > min])

# Filter invalid audio streams:
def filter_invalid_streams():
    return Criterion.all(
        [
            # Filter anonymous content
            claim.publisher_id.notnull(),
            # Blocked channels
            # claim.publisher_id.notin(BLOCKED_CHANNELS),
        ]
    )

# Query for searching all audio content
def bulk_fetch_streams():
    # Basic query
    q = (
        Query.from_(claim)
        .select(
            claim.name,
            claim.title,
            claim.claim_id,
            claim.language,
            claim.created_at,
            claim.modified_at,
            claim.description,
            claim.content_type,
            claim.audio_duration,
            # Fee ( price )
            claim.fee,
            claim.fee_currency,
            # Publisher data
            claim.author,
            claim.publisher_id,
            # license data
            claim.license,
            claim.license_url,
            # Thumbnail
            claim.thumbnail_url,
            # Outpoint data
            claim.transaction_hash_id,
            claim.vout,
        )
        .where(
            filter_by_content_type()
            & filter_by_audio_duration()
            & filter_invalid_streams()
        )
    )
    # returns new query
    return q


# Fix issues with chainquery api and pypika query format:
def formatQuery(q):
    return q.get_sql().replace('"', "")

# Default options for queries
default_query_options = {
    "limit": 100
}

# Function to run a query and retrive data from the chainquery public api
def query(q, options = default_query_options):
    try:
        # Apply options
        q = q.limit(options["limit"])
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

    # Handle connection error
    except req.ConnectionError as error:
        print("[!] Connection error: {0}".format(error))

    # Handle http error ( 404, 503, etc... )
    except req.HTTPError as error:
        print("[!] Http error: {0}".format(error))
