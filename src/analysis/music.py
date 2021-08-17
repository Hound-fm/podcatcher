import pandas as pd
from elastic import Elastic
from elastic.definitions import INDEX
from utils import save_df_cache, load_df_cache
from constants import STREAM_TYPE, CHANNEL_TYPE
from .cache import update_streams_cache, update_channels_cache


def process_music(chunk):
    df_tracks = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["MUSIC"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["MUSIC"])
        & chunk.df_streams.license.notnull()
        & ~chunk.df_streams.license.isin(["None", "none", ""])
    ].copy()
    # No songs on dataset chunk
    if df_tracks.empty:
        return
    # Update songs
    update_streams_cache(df_tracks, INDEX["STREAM"])
    # Update artists
    update_channels_cache(df_tracks, chunk.df_channels, INDEX["CHANNEL"])
