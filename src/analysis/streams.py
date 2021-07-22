import time
import pandas as pd
from sync import sync_streams_data
from utils import get_streams_cannonical_url
from .tags import process_tags


def process_streams(df):
    df_streams = df.copy()
    # Get cannonical_url
    df_streams["cannonical_url"] = get_streams_cannonical_url(df_streams)
    # Get updated data from sdk
    df_streams = sync_streams_data(df_streams)
    # SDK failed to sync data
    if df_streams.empty:
        return df_streams
    # Filter spent / inactive claims
    df_streams = df_streams.loc[df_streams["status"] == "active"]
    # Process stream tags
    df_tags = process_tags(df_streams)
    # Merge tags data
    df_streams = df_streams.drop(columns="tags")
    df_streams = pd.merge(df_streams, df_tags, on="claim_id")
    # Filter uknown types
    df_streams = df_streams.loc[df_streams["stream_type"].notnull()]
    return df_streams
