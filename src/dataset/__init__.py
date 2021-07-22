# Build json datasets from chainquery results
import time
import asyncio
from utils import unique_array, save_json_cache, get_current_time
from chainquery import query, queries
from analysis.constants import DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_INDEX

# Build channles dataset
def build_channels_dataset(id_list):
    results = query(queries.bulk_fetch_channels(id_list))
    save_json_cache(results, "channels")


# Build stream dataset
def build_streams_dataset(
    chunk_index=DEFAULT_CHUNK_INDEX,
    chunk_size=DEFAULT_CHUNK_SIZE,
):
    query_options = {"limit": chunk_size, "offset": chunk_size * chunk_index}
    results = query(queries.bulk_fetch_streams(), query_options)
    save_json_cache(results, "streams")
    return results


# Build data sets
def build_dataset_chunk(chunk_index=DEFAULT_CHUNK_INDEX, chunk_size=DEFAULT_CHUNK_SIZE):
    streams_ids = []
    channels_ids = []
    # Get content
    content = build_streams_dataset(chunk_index, chunk_size)
    content_size = len(content) if content else 0

    # Stop function if there is no content
    if content_size == 0:
        return False

    # Get list of ids
    for item in content:
        streams_ids.append(item["claim_id"])
        channels_ids.append(item["channel_id"])

    if len(channels_ids) > 0:
        channels_ids = unique_array(channels_ids)
        # Merge claim ids
        build_channels_dataset(channels_ids)
        return True
    return False
