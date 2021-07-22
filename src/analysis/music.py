import pandas as pd
from constants import STREAM_TYPE, CHANNEL_TYPE
from utils import save_df_cache, load_df_cache


def merge_artists(df):
    merged = (
        df.groupby("channel_id")
        .agg(
            {
                "trending": "sum",
                "reposted": "sum",
                "channel_title": "first",
            }
        )
        .reset_index()
    )
    return merged


def merge_tracks(df):
    merged = (
        df.groupby("channel_id")
        .agg(
            # Sum trending scores
            trending=pd.NamedAgg(column="trending", aggfunc="sum"),
            # Total reposts
            reposted=pd.NamedAgg(column="reposted", aggfunc="sum"),
            # Count podcast episodes
            tracks=pd.NamedAgg(column="channel_id", aggfunc="count"),
            # Podcast title
            channel_title=pd.NamedAgg(column="channel_title", aggfunc="first"),
        )
        .reset_index()
    )
    return merged


def update_artists_cache(df_update):
    df_merged = df_update
    # Read from local cache
    try:
        # Load cached data
        cached = load_df_cache("df_artists")
        # Merge new data
        df_merged = pd.concat([cached, df_update])
        df_merged = merge_artists(df_merged)
    except FileNotFoundError:
        pass

    if not df_merged.empty:
        # Save to local cache
        save_df_cache(df_merged, "df_artists")


def update_tracks_cache(df_update):
    df_merged = df_update
    # Read from local cache
    try:
        # Load cached data
        cached = load_df_cache("df_tracks")
        # Merge new data
        df_merged = pd.concat([cached, df_update])
    except FileNotFoundError:
        pass

    if not df_merged.empty:
        # Save to local cache
        save_df_cache(df_merged, "df_tracks")


# Process podcast episodes
def process_music(chunk):
    df_tracks = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["MUSIC"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["MUSIC"])
        & chunk.df_streams.license.notnull()
    ]
    # No music recordings on dataset chunk
    if df_tracks.empty:
        return
    # Remove duplicates and merge data
    df_artists = merge_tracks(df_tracks)
    # Update cache
    update_tracks_cache(df_tracks)
    # Update cache
    update_artists_cache(df_artists)
