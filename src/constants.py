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

# Popular audio media types from schema.org
STREAM_TYPES: Final = {"MusicRecording", "PodcastEpisode"}

# Channel types
CHANNEL_TYPES: Final = {"Artist", "PodcastSeries"}

STREAM_TYPE: Final = {
    "MUSIC": "MusicRecording",
    "PODCAST": "PodcastEpisode",
}

CHANNEL_TYPE: Final = {
    "MUSIC": "Artist",
    "PODCAST": "PodcastSeries",
}

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

ELASTIC_INDICES: Final = [
    "artists",
    "podcast_series",
    "music_recordings",
    "podcast_episodes",
]

ELASTIC_INDEX: Final = {
    "ARTISTS": "artists",
    "PODCAST_SERIES": "podcast_series",
    "MUSIC_RECORDINGS": "music_recordings",
    "PODCAST_EPISODES": "podcast_episodes",
}
