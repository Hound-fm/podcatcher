import time
import pandas as pd
from .tags import process_tags
from constants import STREAM_TYPE
from vocabulary import MULTILINGUAL


def process_streams(df):
    df_streams = df.copy()
    # Filter spent / inactive claims
    df_streams = df_streams.loc[df_streams["status"] == "active"]
    # Process stream tags
    df_tags = process_tags(df_streams)
    # Merge tags data
    df_streams = df_streams.drop(columns="tags")
    df_streams = pd.merge(df_streams, df_tags, on="stream_id")
    # Classify untagged content:
    # It is possible to detect podcast episodes by title.
    df_streams.loc[
        df_streams.stream_type.isnull()
        & df_streams.title.str.contains("|".join(MULTILINGUAL["PODCAST"]), case=False),
        "stream_type",
    ] = STREAM_TYPE["PODCAST"]
    # Filter uknown types
    df_streams = df_streams.loc[df_streams["stream_type"].notnull()]

    return df_streams
