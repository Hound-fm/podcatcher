import pandas as pd
from elastic import Elastic
from elastic.definitions import INDEX
from vocabulary import MULTILINGUAL
from constants import STREAM_TYPE, CHANNEL_TYPE
from .cache import update_streams_cache, update_channels_cache


def is_podcast_series(df):
    return df["channel_title"].str.contains(
        "|".join(MULTILINGUAL["PODCAST"]), case=False
    )


def process_podcasts(chunk):
    df_episodes = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["PODCAST"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["PODCAST"])
    ].copy()
    # No podcast episodes on dataset chunk
    if df_episodes.empty:
        return
    # Fix missing values
    df_episodes.loc[df_episodes.license.isnull(), "license"] = "All Rights Reserved"
    # Update podcast episodes cache
    update_streams_cache(df_episodes, INDEX["STREAM"])
    # Update podcast series cache
    update_channels_cache(df_episodes, chunk.df_channels, INDEX["CHANNEL"])
