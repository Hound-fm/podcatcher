import time
import pandas as pd
from utils import now_timestamp, get_permanent_url
from sync import sync_elastic_search, sync_claims_metadata
from config import config
from logger import log
from dataset import build_dataset_chunk
from dataset.loader import Dataset_chunk_loader
from status import main_status
from .channels import process_channels
from .streams import process_streams
from .music import process_music
from .podcasts import process_podcasts

# Global values
dataset_chunk_index = -1
dataset_chunk_size = config["DEFAULT_CHUNK_SIZE"]
delay = config["DEFAULT_TIMEOUT_DELAY"]

# Stop scan
def stop_scan(error=True):
    global dataset_chunk_index
    last_index = (dataset_chunk_index - 1) if (dataset_chunk_index > -1) else -1
    main_status.update_status({"chunk_index": last_index, "updated": now_timestamp()})
    # Handle process error
    if error:
        log.error(f"Failed to process dataset chunk on index: {dataset_chunk_index}")
        main_status.update_status({"sync": False})
        raise SystemExit(0)
    else:
        sync_elastic_search()
        main_status.update_status({"sync": True, "init_sync": True})
        log.info(f"Sync completed!")


# Scan all existent claims
def start_scan():
    global dataset_chunk_index
    dataset_chunk_index += 1
    ready = build_dataset_chunk(dataset_chunk_index, dataset_chunk_size)
    # Build is empty
    if ready == "end":
        stop_scan(False)
    # Build completed
    if ready:
        # Process dataset chunk
        success = process_dataset_chunk()
        # Dataset chunk was processed successfully
        if success:
            log.info(f"Tasks completed for chunk on index: {dataset_chunk_index}")
            # Delay for timeout errors
            time.sleep(delay)
            # Load next dataset chunk
            start_scan()
        else:
            stop_scan()
    else:
        stop_scan()


# Scan claims
def scan(start_index=-1):
    # Reset chunk index
    global dataset_chunk_index
    dataset_chunk_index = start_index
    start_scan()


def process_dataset_chunk():
    # Load dataset chunk
    chunk = Dataset_chunk_loader()
    # Not enough data to process chunk
    if not chunk.valid:
        return False
    # Get cannonical_url
    chunk.df_streams["url"] = get_permanent_url(chunk.df_streams)
    # Get updated metadata from sdk
    metadata = sync_claims_metadata(chunk.df_streams, chunk.df_channels)

    # Sdk failed
    if (
        not metadata
        or (not metadata.keys() & {"streams", "channels"})
        or metadata["streams"].empty
        or metadata["channels"].empty
    ):
        return False

    chunk.df_streams = pd.merge(chunk.df_streams, metadata["streams"], on="stream_id")
    chunk.df_channels = pd.merge(
        chunk.df_channels, metadata["channels"], on="channel_id"
    )

    # Process channels
    chunk.df_channels = process_channels(chunk.df_channels)
    # Process all streams
    chunk.df_streams = process_streams(chunk.df_streams)
    # Merge channel data
    chunk.df_streams = pd.merge(
        chunk.df_streams,
        chunk.df_channels[
            ["channel_id", "channel_name", "channel_title", "channel_type"]
        ],
        on="channel_id",
    )

    # No relevant data found. Skip further analysis.
    if chunk.df_streams.empty:
        return True
    # Process podcast series and episodes
    process_music(chunk)
    process_podcasts(chunk)
    # Success status
    return True
