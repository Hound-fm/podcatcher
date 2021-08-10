import time
import pandas as pd
from sync import sync_elastic_search
from logger import log
from dataset import build_dataset_chunk
from dataset.loader import Dataset_chunk_loader
from status import main_status
from .channels import process_channels
from .streams import process_streams
from .music import process_music
from .podcasts import process_podcasts
from .constants import DEFAULT_CHUNK_SIZE, DEFAULT_TIMOUT_DELAY

# Global values
dataset_chunk_index = -1
dataset_chunk_size = DEFAULT_CHUNK_SIZE
delay = DEFAULT_TIMOUT_DELAY

# Stop scan
def stop_scan(error=True):
    global dataset_chunk_index
    last_index = (dataset_chunk_index - 1) if (dataset_chunk_index > -1) else -1
    main_status.update_status(False, last_index)
    # Handle process error
    if error:
        log.error(f"Failed to process dataset chunk on index: {dataset_chunk_index}")
        raise SystemExit(0)
    else:
        log.info(f"Sync completed!")
        sync_elastic_search()


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
    # Update sync status
    main_status.update_status(False, dataset_chunk_index)


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
    # Process channels
    chunk.df_channels = process_channels(chunk.df_channels)
    if chunk.df_channels.empty:
        return True
    # Merge channes datas
    chunk.df_streams = pd.merge(
        chunk.df_streams,
        chunk.df_channels[
            ["channel_id", "channel_name", "channel_title", "channel_type"]
        ],
        on="channel_id",
    )
    if chunk.df_streams.empty:
        return True
    # Process all streams
    chunk.df_streams = process_streams(chunk.df_streams)
    # Process podcast series and episodes
    if not chunk.df_streams.empty:
        process_music(chunk)
        process_podcasts(chunk)
        # Success status
        return True
    return False
