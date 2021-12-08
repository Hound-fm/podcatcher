import time
import atexit
import pandas as pd
from utils import now_timestamp, get_streams_urls, get_channels_urls
from sync import sync_elastic_search, sync_metadata
from config import config
from logger import console
from dataset import build_dataset_chunk
from dataset.loader import Dataset_chunk_loader
from status import main_status
from .channels import process_channels
from .streams import process_streams
from .music import process_music
from .podcasts import process_podcasts

# Global values
dataset_chunk_index = -1
dataset_chunk_size = config["CHUNK_SIZE"]
delay = config["TIMEOUT_DELAY"]

# Stop scan
def stop_scan(error=True):
    # Stop output of task status
    console.stop_status()

    global dataset_chunk_index
    last_index = (dataset_chunk_index - 1) if (dataset_chunk_index > -1) else -1

    main_status.update_status(
        {"chunk_index": last_index, "updated": now_timestamp().isoformat()}
    )
    # Handle process error
    if error:
        console.error(
            "SYNC", f"Dataset chunk {dataset_chunk_index}", action="Failed to process!"
        )
        console.info("HELP", "Use command 'retry-sync' to fix it.")
    else:
        sync_elastic_search()
        main_status.update_status({"init_sync": True})
        console.info("SYNC", "Sync completed!")
    # Stop process
    raise SystemExit(0)


# Scan all existent claims
def start_scan():
    global dataset_chunk_index
    dataset_chunk_index += 1
    console.update_status(
        f"[green] --- Dataset chunk {dataset_chunk_index} ~ [yellow]Building..."
    )
    ready = build_dataset_chunk(dataset_chunk_index, dataset_chunk_size)

    # Build is empty
    if ready == "end":
        console.stop_status()
        stop_scan(False)
    # Build completed
    if ready:
        console.info(
            "SYNC",
            f"Dataset chunk {dataset_chunk_index}",
            action="Build completed!",
            stop_status=True,
        )
        console.update_status(
            f"[green] --- Dataset chunk {dataset_chunk_index} ~ [yellow]Scanning..."
        )
        # Process dataset chunk
        success = process_dataset_chunk()
        # Dataset chunk was processed successfully
        if success:
            console.info(
                "SYNC",
                f"Dataset chunk {dataset_chunk_index}",
                action="All tasks completed!",
                stop_status=True,
            )
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
    # No relevant data found. Skip further analysis.
    if chunk.valid == "empty":
        return True
    # Get urls
    chunk.df_streams["url"] = get_streams_urls(chunk.df_streams)
    chunk.df_channels["channel_url"] = get_channels_urls(chunk.df_channels)
    # Get updated metadata from sdk
    metadata = sync_metadata(chunk.df_streams, chunk.df_channels)

    # Sdk failed
    if (
        not metadata
        or (not metadata.keys() & {"streams", "channels"})
        or metadata["streams"].empty
        or metadata["channels"].empty
    ):
        return False

    console.info(
        "SYNC",
        f"Dataset chunk {dataset_chunk_index}",
        action="Synced metadata!",
        stop_status=True,
    )

    chunk.df_streams = pd.merge(chunk.df_streams, metadata["streams"], on="stream_id")
    chunk.df_channels = pd.merge(
        chunk.df_channels, metadata["channels"], on="channel_id"
    )

    # Process channels
    chunk.df_channels = process_channels(chunk.df_channels)

    # No relevant data found. Skip further analysis.
    if chunk.df_channels.empty:
        return True

    # Process all streams
    chunk.df_streams = process_streams(chunk.df_streams)
    # Merge channel data
    chunk.df_streams = pd.merge(
        chunk.df_streams,
        chunk.df_channels[
            [
                "channel_id",
                "channel_name",
                "channel_title",
                "channel_type",
                "channel_url",
            ]
        ],
        on="channel_id",
    )

    # Simplify column names
    chunk.df_channels.rename(columns={"channel_url": "url"}, inplace=True)

    # No relevant data found. Skip further analysis.
    if chunk.df_streams.empty:
        return True

    # Process podcast series and episodes
    process_music(chunk)
    process_podcasts(chunk)

    # Success status
    return True


# Handle program exit
def exit_handler():
    console.stop_status()


atexit.register(exit_handler)
