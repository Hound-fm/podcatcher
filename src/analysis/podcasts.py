import pandas as pd
from constants import STREAM_TYPE, CHANNEL_TYPE
from utils import save_df_cache, load_df_cache


def update_podcast_episodes_cache(df_update):
    df_merged = df_update
    # Read from local cache
    try:
        # Load cached data
        cached = load_df_cache("df_podcast_episodes")
        # Merge new data
        df_merged = pd.concat([cached, df_update])
    except FileNotFoundError:
        pass

    if not df_merged.empty:
        # Save to local cache
        save_df_cache(df_merged, "df_podcast_episodes")


def update_podcast_series_cache(df_update):
    df_merged = df_update
    # Read from local cache
    try:
        # Load cached data
        cached = load_df_cache("df_podcast_series")
        # Merge new data
        df_merged = pd.concat([cached, df_update])
        df_merged = merge_podcast_series(df_merged)
    except FileNotFoundError:
        pass

    if not df_merged.empty:
        # Save to local cache
        save_df_cache(df_merged, "df_podcast_series")


def merge_podcast_episodes(df):
    merged = (
        df.groupby("channel_id")
        .agg(
            # Sum trending scores
            trending=pd.NamedAgg(column="trending", aggfunc="sum"),
            # Total reposts
            reposted=pd.NamedAgg(column="reposted", aggfunc="sum"),
            # Count podcast episodes
            episodes=pd.NamedAgg(column="channel_id", aggfunc="count"),
            # Podcast title
            channel_title=pd.NamedAgg(column="channel_title", aggfunc="first"),
        )
        .reset_index()
    )
    return merged


def merge_podcast_series(df):
    merged = (
        df.groupby("channel_id")
        .agg(
            {
                "trending": "sum",
                "reposted": "sum",
                "episodes": "sum",
                "channel_title": "first",
            }
        )
        .reset_index()
    )
    return merged


def process_podcast_series(df_podcasts, chunk):
    df_podcast_series = df_podcasts[
        [
            "trending",
            "reposted",
            "channel_id",
            "channel_title",
        ]
    ]

    # Remove duplicates and merge data
    df_podcast_series = merge_podcast_episodes(df_podcast_series)
    # Update cache
    update_podcast_series_cache(df_podcast_series)


# Process podcast episodes
def process_podcasts(chunk):
    df_podcasts = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["PODCAST"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["PODCAST"])
    ]
    # No podcast episodes on dataset chunk
    if df_podcasts.empty:
        return
    # Update cache
    update_podcast_episodes_cache(df_podcasts)
    # Process podcast series
    process_podcast_series(df_podcasts, chunk)
