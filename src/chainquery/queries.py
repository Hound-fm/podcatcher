# Import dependencies
from pypika import Query, Order, Tables, Criterion, functions as fn
from constants import CLAIM_TYPE, CONTENT_TYPE_AUDIO

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
# Default duration ( Longer than 3 minutes )
def filter_by_audio_duration(min=60 * 3):
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


# Filter invalid channels
def filter_invalid_channels():
    return Criterion.all(
        [
            # Filter invalid title
            claim.title.notnull() & claim.title
            != ""
            # Blocked channels
            # claim.claim_id.notin(BLOCKED_CHANNELS),
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


# Query for searching channels
def bulk_fetch_channels(channels):
    q = (
        Query.from_(claim)
        .select(
            claim.claim_id.as_("publisher_id"),
            claim.name.as_("publisher_name"),
            claim.title.as_("publisher_title"),
            claim.email,
            claim.created_at,
            claim.website_url,
            claim.modified_at,
            claim.description,
            claim.thumbnail_url,
            # Outpoint data
            claim.transaction_hash_id,
            claim.vout,
        )
        .where(claim.claim_id.isin(channels) & filter_invalid_channels())
    )
    # returns new query
    return q


def bulk_fetch_tags(claim_list):
    tag_name = (
        Query.from_(tag).select(tag.tag).where(tag.id == claim_tag.tag_id).limit(1)
    )
    q = (
        Query.from_(claim_tag)
        .select(claim_tag.tag_id, claim_tag.claim_id, tag_name.as_("tag_name"))
        .where(claim_tag.claim_id.isin(claim_list))
    )
    return q
