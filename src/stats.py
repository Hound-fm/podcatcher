import time
import numpy as np
import pandas as pd
import eland as ed
from elastic import Elastic
from utils import save_json_cache, unique_clean_list, assign_empty_list
from constants import STREAM_TYPES
from analysis.cache import update_streams_cache
from analytics import fetch_stream_analytics


def get_channel_genres(df_streams):
    df_genres = df_streams.explode("genres").dropna(axis="index")
    df_genres = df_genres.rename(columns={"genres": "label"})
    df_channel_genres = (
        df_genres[["channel_id", "label"]]
        .groupby(["channel_id", "label"], sort=False)
        .agg(frequency=("label", "count"))
        .reset_index()
    )
    df_channel_genres = df_channel_genres.sort_values(by=["frequency"], ascending=False)
    df_channel_genres = (
        df_channel_genres.groupby(["channel_id"])
        .agg(stream_genres=("label", unique_clean_list))
        .reset_index()
    )

    df_channel_genres = df_channel_genres.set_index("channel_id")

    return df_channel_genres


def update_channel_content_genres():
    el = Elastic()
    el.update_index_mapping("channel", {"content_genres": {"type": "keyword"}})
    df_streams = el.get_df("stream", ["channel_id", "genres"])
    df_channels = el.get_df("channel", None)
    genres = get_channel_genres(df_streams)
    # Initialize empty list
    df_channels["content_genres"] = assign_empty_list(df_channels)
    df_channels["content_genres"].update(genres["stream_genres"])
    el.append_df("channel", df_channels)


def get_usage_df(df, column):
    df_tags = df.explode(column).dropna(axis="index")
    df_tags = df_tags.rename(columns={f"{column}": "label"})
    df_tags = (
        df_tags.groupby(["label"], sort=False)
        .agg(frequency=("label", "count"), reach=("channel_id", "nunique"))
        .reset_index()
    )
    df_tags = df_tags.sort_values(by=["frequency", "reach"], ascending=False)
    return df_tags


def update_stream_genres(stream_type):
    el = Elastic()
    df_streams = el.get_df("stream", ["tags", "genres", "stream_type", "channel_id"])
    df_streams = df_streams[df_streams.stream_type == stream_type]
    genres = get_usage_df(df_streams, "genres")
    genres["category_type"] = stream_type
    genres["genre_id"] = genres["label"]
    el.append_df_chunk("genre", genres)


def update_stream_analytics():
    el = Elastic()
    df_stream = el.get_df("stream", None)
    df_stream = fetch_stream_analytics(df_stream)
    el.append_df("stream", df_stream)


def fetch_stats():
    for stream_type in STREAM_TYPES:
        # Update genre index
        update_stream_genres(stream_type)
    update_stream_analytics()
    update_channel_content_genres()
