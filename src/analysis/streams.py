import time
import pandas as pd
from constants import STREAM_TYPE
from vocabulary import MULTILINGUAL
from .tags import process_tags
from .music import is_artist
from .description import process_stream_description


def process_untagged_streams(df):
    df_streams = df.copy()
    # Untagged content
    df_untagged = df_streams[
        (~df_streams["stream_type"].notnull()) & df_streams["channel_type"].notnull()
    ].copy()

    # Analysis for untagged content
    if not df_untagged.empty:
        # Extract metadata from description
        df_description_metadata = process_stream_description(df_untagged)
        df_streams = df_streams.drop(columns=["description"])

        if not df_description_metadata.empty:
            # Merge metadata from description
            df_streams = pd.merge(
                df_streams, df_description_metadata, on="stream_id", how="left"
            )

            merged_columns = ["stream_type", "stream_metadata_score", "genres"]

            for merged_column in merged_columns:
                prefixed_x = merged_column + "_x"
                prefixed_y = merged_column + "_y"

                if {prefixed_x, prefixed_y}.issubset(df_streams.columns):
                    # Remove prefix
                    df_streams = df_streams.rename(
                        columns={f"{prefixed_x}": merged_column}
                    )
                    # Use new values
                    if merged_column == "stream_metadata_score":
                        df_streams.loc[
                            df_streams[merged_column] <= 0, merged_column
                        ] = df_streams[prefixed_y]
                    if merged_column == "genres":
                        df_streams.loc[
                            df_streams[prefixed_y].notnull(), merged_column
                        ] = df_streams[prefixed_y]
                    else:
                        df_streams.loc[
                            ~df_streams[merged_column].notnull(), merged_column
                        ] = df_streams[prefixed_y]
                    # Drop unecessary columns
                    df_streams = df_streams.drop(columns=prefixed_y)

    # Filter uknown types
    return df_streams.loc[
        df_streams["stream_type"].notnull() & df_streams["channel_type"].notnull()
    ]


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
    # -- Classify as music based on trusted music channels:
    df_streams.loc[
        df_streams.stream_type.isnull() & is_artist(df_streams),
        "stream_type",
    ] = STREAM_TYPE["MUSIC"]

    # -- It is possible to detect podcast episodes by title.
    df_streams.loc[
        df_streams.stream_type.isnull()
        & df_streams.title.str.contains(
            "|".join(MULTILINGUAL["PODCAST"] + MULTILINGUAL["EPISODE"]), case=False
        ),
        "stream_type",
    ] = STREAM_TYPE["PODCAST"]

    return df_streams
