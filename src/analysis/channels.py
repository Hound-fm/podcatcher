import os
import json
import pandas as pd
from constants import CHANNEL_TYPE
from vocabulary import MULTILINGUAL
from .tags import process_tags
from .music import is_artist
from .podcasts import is_podcast_series


def process_channels(df):
    # Check channel type
    df_channels = df.copy()

    # Process channels tags
    df_tags = process_tags(df_channels, "channel")

    # Merge tags data
    df_channels = df_channels.drop(columns="tags")
    df_channels = pd.merge(df_channels, df_tags, on="channel_id")

    # Fix untagged content
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
