import pandas as pd
from constants import STREAM_TYPE, CHANNEL_TYPE, ELASTIC_INDEX
from utils import save_df_cache, load_df_cache
from .cache import update_streams_cache, update_channels_cache


def process_music(chunk):
    df_tracks = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["MUSIC"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["MUSIC"])
        & chunk.df_streams.license.notnull()
    ].copy()
    # No songs on dataset chunk
    if df_tracks.empty:
        return
    # Update songs
    update_streams_cache(df_tracks, ELASTIC_INDEX["MUSIC_RECORDINGS"])
    # Update artists
    update_channels_cache(df_tracks, chunk.df_channels, ELASTIC_INDEX["ARTISTS"])
