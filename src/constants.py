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

# Channel types

STREAM_TYPE: Final = {
    "MUSIC": "music_recording",
    "PODCAST": "podcast_episode",
}

STREAM_TYPES: Final = set(STREAM_TYPE.values())

CHANNEL_TYPE: Final = {
    "MUSIC": "artist",
    "PODCAST": "podcast_series",
}

CHANNEL_TYPES: Final = set(CHANNEL_TYPE.values())

# Popular irrelevant tags
FILTER_TAGS: Final = [
    "audio",
    "audiobook",
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
