# Import dependencies
import os
import json
from config import config
from utils import now_timestamp
from pypika import Query, Order, Tables, Criterion, Order, functions as fn
from constants import CLAIM_TYPE, CONTENT_TYPE_AUDIO
from status import main_status

# Load block list
with open(config["BLOCK_LIST"], "r") as f:
    BLOCK_LIST = json.load(f)

# Blocked channels list
BLOCKED_CHANNELS = BLOCK_LIST["CHANNELS"]

# Tables definition
claim, support = Tables("CLAIM", "SUPPORT")


def filter_bid_state():
    return Criterion.all(
        [
            claim.bid_state.notnull(),
            claim.bid_state != "Spent",
            claim.bid_state != "Expired",
        ]
    )


# Filter all non-audio content
def filter_by_content_type(content_type=CONTENT_TYPE_AUDIO):
    return Criterion.all(
        [
            claim.type == CLAIM_TYPE["STREAM"],
            claim.content_type.like(content_type + "%"),
        ]
    )


# Filter all content by audio duration:
# Default duration ( Longer than 45 seconds )
def filter_by_audio_duration(min=45):
    return Criterion.all([claim.audio_duration.notnull(), claim.audio_duration >= min])


# Filter invalid audio streams:
def filter_invalid_streams():
    return Criterion.all(
        [
            # Filter expired claims
            filter_bid_state(),
            # Filter anonymous content
            claim.publisher_id.notnull(),
            # Blocked channels
            claim.publisher_id.notin(BLOCKED_CHANNELS),
        ]
    )


# Filter invalid audio streams:
def filter_invalid_reposts():
    return Criterion.all(
        [
            # Filter expired claims
            filter_bid_state(),
            # Filter anonymous content
            claim.publisher_id.notnull(),
            # Blocked channels
            claim.publisher_id.notin(BLOCKED_CHANNELS),
        ]
    )


# Filter invalid channels
def filter_invalid_channels():
    return Criterion.all(
        [
            # Filter expired claims
            filter_bid_state(),
            # Filter invalid title
            claim.title.notnull() & claim.title != "",
            # Blocked channels
            claim.claim_id.notin(BLOCKED_CHANNELS),
        ]
    )


# Query for searching all audio content
def bulk_fetch_streams():
    # Basic query
    q = Query.from_(claim).select(
        claim.name,
        claim.title,
        claim.claim_id.as_("stream_id"),
        claim.audio_duration.as_("duration"),
        # Publisher data
        claim.publisher_id.as_("channel_id"),
        # Outpoint data
        claim.transaction_hash_id,
        claim.vout,
        # Updated time
        claim.modified_at,
    )

    # Chainquery public api doesn't support this query anymore: Timeout error
    # Full sync completed. Search for recent streams.
    # current = main_status.status
    # if current["init_sync"] and current["updated"]:
    # q = q.where(
    # filter_by_content_type()
    # & filter_by_audio_duration()
    # & filter_invalid_streams()
    # & (claim.modified_at >= current["updated"])
    # )

    # Search for all streams
    q = q.where(
        filter_by_content_type() & filter_by_audio_duration() & filter_invalid_streams()
    )

    # returns new query
    return q


# Query for fetching reposts
def bulk_fetch_reposts(claim_list):
    # Basic query
    q = (
        Query.from_(claim)
        .select(
            claim.created_at.as_("event_date"),
            claim.claim_reference.as_("event_stream_id"),
            claim.publisher_id.as_("author_id"),
        )
        .where(
            (claim.type == "claimreference")
            & claim.claim_reference.isin(claim_list)
            & filter_invalid_reposts()
        )
    )
    # returns new query
    return q


# Query for searching channels
def bulk_fetch_channels(channels):
    q = (
        Query.from_(claim)
        .select(
            claim.name.as_("channel_name"),
            claim.title.as_("channel_title"),
            claim.claim_id.as_("channel_id"),
            # Outpoint data
            claim.transaction_hash_id,
            claim.vout,
            # Updated time
            claim.modified_at,
        )
        .where(claim.claim_id.isin(channels) & filter_invalid_channels())
    )
    # returns new query
    return q
