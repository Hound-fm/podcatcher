import os
import json
import pandas as pd
from config import config
from sync import sync_channels_data
from utils import get_channels_cannonical_url
from constants import CHANNEL_TYPE
from vocabulary import MULTILINGUAL
from .tags import process_tags

# Load block list
with open(config["SAFE_LIST"], "r") as f:
    SAFE_LIST = json.load(f)


def is_artist(df):
    return df["channel_id"].isin(SAFE_LIST["ARTISTS"])


def is_podcast_series(df):
    return df["channel_title"].str.contains(
        "|".join(MULTILINGUAL["PODCAST"]), case=False
    )


def process_channels(df):
    # Check channel type
    df_channels = df.copy()
    # Get cannonical_url
    df_channels["cannonical_url"] = get_channels_cannonical_url(df_channels)
    # Sync sdk data
    df_channels = sync_channels_data(df_channels)
    # SDK failed to sync data
    if df_channels.empty:
        return df_channels

    # Process channels tags
    df_tags = process_tags(df_channels, "channel")

    # Merge tags data
    df_channels = df_channels.drop(columns="tags")

    df_channels = pd.merge(df_channels, df_tags, on="channel_id")

    df_channels.loc[is_artist(df_channels), "channel_type"] = CHANNEL_TYPE["MUSIC"]
    df_channels.loc[is_podcast_series(df_channels), "channel_type"] = CHANNEL_TYPE[
        "PODCAST"
    ]

    # Filter uknown types
    df_channels = df_channels.loc[
        df_channels["channel_type"].notnull() & df_channels["channel_title"].notnull()
    ]

    # Filter suspicious channels (keep safe list)
    keep_safe_list = is_artist(df_channels) | is_podcast_series(df_channels)
    df_channels = df_channels.loc[
        keep_safe_list
        | ~df_channels.channel_title.str.contains(
            "|".join(MULTILINGUAL["MUSIC"]), case=False
        )
    ]
    return df_channels
