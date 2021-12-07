from typing import Final

# Static values
INDEX: Final = {"STREAM": "stream", "CHANNEL": "channel", "GENRE": "genre"}
INDICES: Final = set(INDEX.values())

# Mappings templates
MAPPINGS_GENRE: Final = {
    "label": "search_as_you_type",
}

# Pandas to eland
MAPPINGS_CHANNEL: Final = {
    "tags": "keyword",
    "genres": "keyword",
    "thumbnail": "text",
    "channel_name": "text",
    "channel_title": "search_as_you_type",
    "channel_type": "keyword",
}

MAPPINGS_STREAM: Final = {
    "title": "search_as_you_type",
    "name": "text",
    "tags": "keyword",
    "genres": "keyword",
    "thumbnail": "text",
    "reposted": "integer",
    "license": "text",
    "channel_id": "text",
    "channel_name": "text",
    "channel_title": "search_as_you_type",
    "stream_type": "keyword",
    "fee_amount": "float",
    "view_count": "integer",
    "likes_count": "integer",
}
