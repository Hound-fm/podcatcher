from typing import Final

# Audio codec formats
AUDIO_FORMATS: Final = {
    "AAC": "aac",
    "MP3": "mp3",
    "WAV": "wav",
    "FLAC": "flac",
    "OGG": "ogg-vorbis",
}

# Audio mime types
MIME_TYPES: Final = {
    "AAC": ["audio/aac", "audio/mp4"],
    "MP3": ["audio/mpeg", "audio/mpa"],
    "OGG": ["audio/ogg", "audio/vorbis", "audio/vorbis-config"],
    "WAV": ["audio/vnd.wave", "audio/wav", "audio/wave", "audio/x-wav"],
    "FLAC": ["audio/flac"],
}

# Constants values
TAG_ID: Final = {
    "MATURE": [1],
    "PODCAST": [1847, 1717, 2900186],
}

# Claim types
CLAIM_TYPE: Final = {
    "STREAM": "stream",
    "CHANNEL": "channel",
    "CLAIM_LIST": "claimList",
    "CLAIM_REPOST": "claimreference",
}

# Content media type
CONTENT_TYPE_AUDIO: Final = "audio"

# Public chainquery API:
CHAINQUERY_API: Final = "https://chainquery.lbry.io/api/sql"

# Public lbry.com api
LBRY_API: Final = "http://localhost:5279"
LBRY_TOKEN: Final = "3Nk17Zn6MruF1N9KkbLAKLHFgnmu377M"
LBRY_COM_API: Final = "https://api.lbry.com/"

# Popular audio media types from schema.org
STREAM_TYPES: Final = {"MusicRecording", "Audiobook", "PodcastEpisode"}

# Channel types
CHANNEL_TYPES: Final = {"Artist", "Audiobook", "PodcastSeries"}

STREAM_TYPE: Final = {
    "MUSIC": "MusicRecording",
    "PODCAST": "PodcastEpisode",
    "AUDIOBOOK": "Audiobook",
}

CHANNEL_TYPE: Final = {
    "MUSIC": "Artist",
    "PODCAST": "PodcastSeries",
    "AUDIOBOOK": "Audiobook",
}

# Popular irrelevant tags
FILTER_TAGS: Final = [
    "audio",
    "audiobooks",
    "lbrivox",
    "book",
    "books",
    "full",
    "free",
    "freemusic",
    "audio library",
    "ncm",
    "new music",
    "nocopyrightmusic",
]
