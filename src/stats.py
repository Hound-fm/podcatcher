import time
import numpy as np
import pandas as pd
import eland as ed
from elastic import Elastic
from utils import save_json_cache
from constants import STREAM_TYPES
from analysis.cache import update_streams_cache
from analytics import fetch_stream_analytics


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
