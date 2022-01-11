import os
import json
import pandas as pd
from constants import CHANNEL_TYPE
from vocabulary import MULTILINGUAL
from .tags import process_tags
from .music import is_artist
from .podcasts import is_podcast_series
from .description import process_channel_description


def process_channels(df):
    # Check channel type
    df_channels = df.copy()
    # Early filters
    # All channels must have a valid title
    df_channels["channel_title"] = df_channels["channel_title"].astype(str)

    df_channels = df_channels.loc[
        df_channels["channel_title"].notnull() & (df_channels["channel_title"] != "")
    ]

    # Filter no thumbnails
    # Note: All channels should have a thumbnail
    # Note: Only channels on safe list will ignore this filter.
    # For creators with blindness or vision impairment is probably not a good idea to enforce this.
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

    # Filter empty or invalid descriptions
    MIN_DESCRIPTION_LENGTH = 5
    df_channels = df_channels.loc[keep_safe_list | df_channels.description.notnull()]
    df_channels.description = df_channels.description.astype(str).str.strip()
    df_channels = df_channels.loc[
        keep_safe_list
        | (
            (df_channels.description != "")
            & (df_channels.description.str.len() > MIN_DESCRIPTION_LENGTH)
        )
    ]

    # Skip further analysis
    if df_channels.empty:
        return df_channels

    # Process channels tags
    df_tags = process_tags(df_channels, "channel")

    # Merge tags data
    df_channels = df_channels.drop(columns="tags")
    df_channels = pd.merge(df_channels, df_tags, on="channel_id")

    if df_channels.empty:
        return df_channels

    # Fix untagged content
    df_channels.loc[is_artist(df_channels), "channel_type"] = CHANNEL_TYPE["MUSIC"]
    df_channels.loc[is_podcast_series(df_channels), "channel_type"] = CHANNEL_TYPE[
        "PODCAST"
    ]

    # Untagged content
    df_untagged = df_channels[~df_channels["channel_type"].notnull()].copy()

    # Analysis for untagged content
    if not df_untagged.empty:
        # Extract metadata from description
        df_description_metadata = process_channel_description(df_untagged)

        if not df_description_metadata.empty:
            # Merge metadata from description
            df_channels = pd.merge(
                df_channels, df_description_metadata, on="channel_id", how="left"
            )

            merged_columns = ["channel_type", "channel_metadata_score", "genres"]

            for merged_column in merged_columns:
                prefixed_x = merged_column + "_x"
                prefixed_y = merged_column + "_y"

                if {prefixed_x, prefixed_y}.issubset(df_channels.columns):
                    # Remove prefix
                    df_channels = df_channels.rename(
                        columns={f"{prefixed_x}": merged_column}
                    )
                    # Use new values
                    if merged_column == "channel_metadata_score":
                        df_channels.loc[
                            df_channels[merged_column] <= 0, merged_column
                        ] = df_channels[prefixed_y]
                    if merged_column == "genres":
                        df_channels.loc[
                            df_channels[prefixed_y].notnull(), merged_column
                        ] = df_channels[prefixed_y]
                    else:
                        df_channels.loc[
                            ~df_channels[merged_column].notnull(), merged_column
                        ] = df_channels[prefixed_y]
                    # Drop unecessary columns
                    df_channels = df_channels.drop(columns=prefixed_y)

    # Filter uknown types
    # Note: All channels should have a valid type ( artist or podcast_series )
    df_channels = df_channels.loc[df_channels["channel_type"].notnull()]

    return df_channels
