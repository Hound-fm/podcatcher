import json
import pandas as pd
from elastic import Elastic
from elastic.definitions import INDEX
from config import config
from utils import save_df_cache, load_df_cache
from constants import STREAM_TYPE, CHANNEL_TYPE
from .cache import update_streams_cache, update_channels_cache

# Load block list
with open(config["SAFE_LIST"], "r") as f:
    SAFE_LIST = json.load(f)


def is_artist(df):
    return df["channel_id"].isin(SAFE_LIST["ARTISTS"])


def format_artist_title(channel_title):
    # Format channel title:
    artist_title = channel_title.str.strip()
    # Note: Adding "music" to the title is irrelevant and will be blocked by the filters.
    # Simplify artist name: "Mozart's Music" -> "Mozart"
    artist_title = artist_title.str.replace("'s music", "")
    artist_title = artist_title.str.replace("'s Music", "")
    # Simplify artist name: "Ludwig van Beethoven music" -> "Ludwig van Beethoven"
    artist_title = artist_title.str.replace(" music", "")
    artist_title = artist_title.str.replace(" Music", "")
    return artist_title


def format_track_title(df_streams):
    df_tracks = df_streams.copy()
    df_tracks.title = df_tracks.title.str.strip()
    title_split = df_tracks.title.str.lower().str.split(pat=" - ", n=3).str
    title_raw_split = df_tracks.title.str.split(pat=" - ", n=3).str
    artist_title = df_tracks.channel_title.str.lower()
    # Remove channel name from stream title: "Mozart - Epic orchesta" -> "Epic orchesta"
    df_tracks.loc[
        (title_split.len() >= 2) & (title_split[0] == artist_title),
        "title",
    ] = title_raw_split[1]
    # Remove channel name from stream title: "Epic orchesta - Mozart" -> "Epic orchesta"
    df_tracks.loc[
        (title_split.len() >= 2) & (title_split[1] == artist_title),
        "title",
    ] = title_raw_split[0]

    return df_tracks.title


def process_music(chunk):

    df_tracks = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["MUSIC"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["MUSIC"])
    ].copy()

    missing_license = ~df_tracks.license.notnull() | df_tracks.license.isin(
        ["None", "none", ""]
    )

    # Auto fill missing unlicensed for truested artists:
    df_tracks.loc[
        missing_license & is_artist(df_tracks), "license"
    ] = "All rights reserved."

    # Filter unlicensed content
    df_tracks = df_tracks[
        df_tracks.license.notnull() & ~df_tracks.license.isin(["None", "none", ""])
    ]

    # No songs on dataset chunk
    if df_tracks.empty:
        return

    # Format channel title:
    chunk.df_channels.channel_title = format_artist_title(
        chunk.df_channels.channel_title
    )
    df_tracks.channel_title = format_artist_title(df_tracks.channel_title)

    # Format stream title:
    df_tracks.title = format_track_title(df_tracks[["title", "channel_title"]])

    # Update songs
    update_streams_cache(df_tracks, INDEX["STREAM"])

    # Update artists
    update_channels_cache(df_tracks, chunk.df_channels, INDEX["CHANNEL"])
