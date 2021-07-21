import pandas as pd
from constants import STREAM_TYPE, CHANNEL_TYPE
from vocabulary import MULTILINGUAL
from utils import save_df_cache, load_df_cache


def is_podcast_series(df):
    return df["publisher_title"].str.contains(
        "|".join(MULTILINGUAL["podcast"]), case=False
    )


def merge_podcast_episodes(df):
    merged = (
        df.groupby("publisher_id")
        .agg(
            # Sum trending scores
            trending=pd.NamedAgg(column="trending", aggfunc="sum"),
            # Total reposts
            reposted=pd.NamedAgg(column="reposted", aggfunc="sum"),
            # Count podcast episodes
            episodes=pd.NamedAgg(column="publisher_id", aggfunc="count"),
            # Podcast title
            podcast_title=pd.NamedAgg(column="publisher_title", aggfunc="first"),
        )
        .reset_index()
    )
    return merged


def merge_podcast_series(df):
    merged = (
        df.groupby("publisher_id")
        .agg(
            {
                "trending": "sum",
                "reposted": "sum",
                "episodes": "sum",
                "podcast_title": "first",
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
            "publisher_id",
            "publisher_title",
        ]
    ]

    # Remove duplicates and merge data
    df_podcast_series = merge_podcast_episodes(df_podcast_series)

    # Read from local cache
    try:
        # Load cached data
        cached = load_df_cache("df_podcast_series")
        # Merge new data
        df_podcast_series = pd.concat([cached, df_podcast_series])
        df_podcast_series = merge_podcast_series(df_podcast_series)
    except FileNotFoundError:
        pass

    # Save to local cache
    save_df_cache(df_podcast_series, "df_podcast_series")


def process_podcasts(chunk):
    df_podcasts = chunk.df_streams[
        (chunk.df_streams.stream_type == STREAM_TYPE["PODCAST_EPISODE"])
        & (chunk.df_streams.channel_type == CHANNEL_TYPE["PODCAST_SERIES"])
    ]
    # No podcast episodes on dataset chunk
    if df_podcasts.empty:
        return
    # Test
    process_podcast_series(df_podcasts, chunk)
