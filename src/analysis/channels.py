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
    # Note: All channels should have a valid type ( artist or podcast_series )
    df_channels = df_channels.loc[
        df_channels["channel_type"].notnull() & df_channels["channel_title"].notnull()
    ]

    # Filter no thumbnails
    # Note: All channels should have a thumbnail
    # Note: Only channels on safe list will ignore this filter.
    # Todo: For creators with blindness or vision impairment is probably not a good idea to enforce this.
    df_channels = df_channels.loc[
        is_artist(df_channels) | df_channels["thumbnail"].str.strip().astype(bool)
    ]

    # Filter channel keywords
    # Filter suspicious channels ( "Free music", "All music", etc.. )
    # Note: Only channels on safe list will ignore this filter.
    # Todo: Add more keywords.
    keep_safe_list = is_artist(df_channels) | is_podcast_series(df_channels)
    df_channels = df_channels.loc[
        keep_safe_list
        | ~df_channels.channel_title.str.contains(
            "|".join(MULTILINGUAL["MUSIC"]), case=False
        )
    ]

    return df_channels
