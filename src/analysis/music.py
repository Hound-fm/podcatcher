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
    # Update songs
    update_streams_cache(df_tracks, INDEX["STREAM"])
    # Update artists
    update_channels_cache(df_tracks, chunk.df_channels, INDEX["CHANNEL"])
