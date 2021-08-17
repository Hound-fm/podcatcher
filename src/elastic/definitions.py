# Static values
INDEX = {"STREAM": "stream", "CHANNEL": "channel"}
INDICES = set(INDEX.values())

# Autocomplete fields
FIELDS_STREAM_AUTOCOMPLETE = [
    "name",
    "title",
    "duration",
    "thumbnail",
    "channel_title",
]

FIELDS_CHANNEL_AUTOCOMPLETE = [
    "thumbnail",
    "channel_name",
    "channel_title",
]

# Mappings templates
MAPPINGS_STREAM_AUTOCOMPLETE = {
    "genre": {"type": "search_as_you_type"},
    "title": {"type": "search_as_you_type"},
}

MAPPINGS_CHANNEL_AUTOCOMPLETE = {
    "genre": {"type": "search_as_you_type"},
    "channel_title": {"type": "search_as_you_type"},
}

# Pandas to eland
MAPPINGS_CHANNEL = {
    "trending": "float",
    "thumbnail": "text",
    "channel_name": "text",
    "channel_title": "text",
    "channel_type": "keyword",
}

MAPPINGS_STREAM = {
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
