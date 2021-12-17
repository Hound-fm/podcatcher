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
    chunk.df_channels.channel_title = chunk.df_channels.channel_title.str.strip()
    # Simplify artist name: "Ludwig van Beethoven music" -> "Ludwig van Beethoven"
    # Note: Adding "music" to the title is irrelevant and will be blocked by the filters.
    chunk.df_channels.channel_title = chunk.df_channels.channel_title.str.replace(
        " music", ""
    )
    chunk.df_channels.channel_title = chunk.df_channels.channel_title.str.replace(
        " Music", ""
    )
    # Simplify artist name: "Mozart's Music" -> "Mozart"
    chunk.df_channels.channel_title = chunk.df_channels.channel_title.str.replace(
        "'s music", ""
    )
    chunk.df_channels.channel_title = chunk.df_channels.channel_title.str.replace(
        "'s Music", ""
    )
    # Format stream title:
    df_tracks.title = df_tracks.title.str.strip()

    title_split = df_tracks.title.str.lower()
    title_split = df_tracks.title.str.split(pat=" - ", n=3).str
    title_raw_split = df_tracks.title.str.split(pat=" - ", n=3).str
    # Remove channel name from stream title: "Mozart - Epic orchesta" -> "Epic orchesta"
    df_tracks.loc[
        (title_split.len() == 2)
        & (
            (title_split[0] == df_tracks.channel_title)
            | (title_split[0] == "original music")
        ),
        "title",
    ] = title_raw_split[1]
    # Remove channel name from stream title: "Epic orchesta - Mozart" -> "Epic orchesta"
    df_tracks.loc[
        (title_split.len() == 2)
        & (
            (title_split[1] == df_tracks.channel_title)
            | (title_split[1] == "original music")
        ),
        "title",
    ] = title_raw_split[0]

    # Update songs
    update_streams_cache(df_tracks, INDEX["STREAM"])
    # Update artists
    update_channels_cache(df_tracks, chunk.df_channels, INDEX["CHANNEL"])
