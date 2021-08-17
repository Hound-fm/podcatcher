from typing import Final

# Static values
INDEX: Final = {"STREAM": "stream", "CHANNEL": "channel"}
INDICES: Final = set(INDEX.values())

# Autocomplete fields
FIELDS_STREAM_AUTOCOMPLETE: Final = [
    "name",
    "title",
    "duration",
    "thumbnail",
    "channel_title",
]

FIELDS_CHANNEL_AUTOCOMPLETE: Final = [
    "thumbnail",
    "channel_name",
    "channel_title",
]

# Mappings templates
MAPPINGS_STREAM_AUTOCOMPLETE: Final = {
    "genre": {"type": "search_as_you_type"},
    "title": {"type": "search_as_you_type"},
}

MAPPINGS_CHANNEL_AUTOCOMPLETE: Final = {
    "genre": {"type": "search_as_you_type"},
    "channel_title": {"type": "search_as_you_type"},
}

# Pandas to eland
MAPPINGS_CHANNEL: Final = {
    "trending": "float",
    "thumbnail": "text",
    "channel_name": "text",
    "channel_title": "text",
    "channel_type": "keyword",
}

MAPPINGS_STREAM: Final = {
    "title": "text",
    "name": "text",
    "tags": "keyword",
    "genres": "keyword",
    "trending": "float",
    "thumbnail": "text",
    "reposted": "integer",
    "license": "text",
    "channel_id": "text",
    "channel_name": "text",
    "channel_title": "text",
    "stream_type": "keyword",
}
