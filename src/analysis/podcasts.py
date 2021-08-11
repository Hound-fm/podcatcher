import pandas as pd
from constants import STREAM_TYPE, CHANNEL_TYPE, ELASTIC_INDEX
from .cache import update_streams_cache, update_channels_cache


def process_podcasts(chunk):
    df_podcasts = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["PODCAST"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["PODCAST"])
    ].copy()
    # No podcast episodes on dataset chunk
    if df_podcasts.empty:
        return
    # Fix missing values
    df_podcasts.loc[df_podcasts.license.isnull(), "license"] = "All Rights Reserved"
    # Update podcast episodes cache
    update_streams_cache(df_podcasts, ELASTIC_INDEX["PODCAST_EPISODES"])
    # Update podcast series cache
    update_channels_cache(
        df_podcasts, chunk.df_channels, ELASTIC_INDEX["PODCAST_SERIES"]
    )
