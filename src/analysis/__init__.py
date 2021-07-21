import time
import asyncio
import pandas as pd
from logger import log
from dataset import build_dataset_chunk
from dataset.loader import Dataset_chunk_loader
from constants import STREAM_TYPE, CHANNEL_TYPE
from sync import sync_stream_data
from status import update_status
from .tags import process_tags
from .podcasts import process_podcasts, is_podcast_series

# Global values
dataset_chunk_index = 45
dataset_chunk_size = 1000
delay = 30

# Scan all existent claims
def full_scan():
    global dataset_chunk_size
    global dataset_chunk_index
    dataset_chunk_index += 1
    ready = build_dataset_chunk(dataset_chunk_index, dataset_chunk_size)

    if ready:
        # Process dataset chunk
        success = process_dataset_chunk()
        # Dataset chunk was processed successfully
        if success:
            log.info(f"Tasks completed for chunk on index: {dataset_chunk_index}")
            # Delay for timeout errors
            time.sleep(delay)
            # Load next dataset chunk
            full_scan()
        else:
            # Handle process error
            log.error(
                f"Failed to process dataset chunk on index: {dataset_chunk_index}"
            )
    else:
        # Handle load error
        log.error(f"Failed to load dataset chunk on index: {dataset_chunk_index}")

    # Update sync status
    update_status(False, dataset_chunk_index)


def process_dataset_chunk():
    # Load dataset chunk
    chunk = Dataset_chunk_loader()
    # Not enough data to process chunk
    if not chunk.valid:
        return False
    # Check channel type
    chunk.df_channels.loc[
        is_podcast_series(chunk.df_channels), "channel_type"
    ] = CHANNEL_TYPE["PODCAST_SERIES"]

    # Merge channel data
    chunk.df_streams = pd.merge(
        chunk.df_streams,
        chunk.df_channels[
            ["publisher_id", "publisher_name", "publisher_title", "channel_type"]
        ],
        on="publisher_id",
    )
    # Get cannonical_url
    chunk.df_streams["cannonical_url"] = get_cannonical_url(chunk.df_streams)
    # Get updated data from sdk
    chunk.df_streams = sync_stream_data(chunk.df_streams)
    # SDK failed to sync data
    if chunk.df_streams.empty:
        return False
    # Filter spent / inactive claims
    chunk.df_streams = chunk.df_streams.loc[chunk.df_streams["status"] == "active"]
    # Process stream tags
    df_tags = process_tags(chunk.df_streams)
    # Merge tags data
    chunk.df_streams = chunk.df_streams.drop(columns="tags")
    chunk.df_streams = pd.merge(chunk.df_streams, df_tags, on="claim_id")
    # Filter uknown types
    chunk.df_streams = chunk.df_streams.loc[chunk.df_streams["stream_type"].notnull()]
    # Podcasts
    process_podcasts(chunk)
    return True


def get_cannonical_url(df):
    df_claims = df.copy()
    df_claims = df_claims[["claim_id", "name", "publisher_name", "publisher_id"]]
    df_claims["claim_char"] = df_claims["claim_id"].str[0]
    df_claims["publisher_char"] = df_claims["publisher_id"].str.slice(0, 2)
    df_claims["cannonical_url"] = (
        +df_claims["publisher_name"].astype(str)
        + "#"
        + df_claims["publisher_char"].astype(str)
        + "/"
        + df_claims["name"].astype(str)
        + "#"
        + df_claims["claim_char"].astype(str)
    )
    df_claims["cannonical_url"] = df_claims["cannonical_url"].astype(str)
    return df_claims["cannonical_url"]
