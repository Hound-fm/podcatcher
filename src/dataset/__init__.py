# Build json datasets from chainquery results
import time
import asyncio
from utils import unique_array, write_json_file, get_current_time
from chainquery import query, queries

from constants import (
    STREAM_TYPE,
    FILTER_TAGS,
)

# Global default values
default_chunk_index = 0
default_chunk_size = 100

# Build tags dataset
# async def build_tags_dataset(id_list):
# results = query(queries.bulk_fetch_tags(id_list))
# write_json_file(results, "tags.json")


# Build channles dataset
def build_channels_dataset(id_list):
    results = query(queries.bulk_fetch_channels(id_list))
    write_json_file(results, "channels.json")


# Build stream dataset
def build_streams_dataset(
    chunk_index=default_chunk_index,
    chunk_size=default_chunk_size,
):
    query_options = {"limit": chunk_size, "offset": chunk_size * chunk_index}
    results = query(queries.bulk_fetch_streams(), query_options)
    write_json_file(results, "streams.json")
    return results


# Build data sets
def build_dataset_chunk(chunk_index=default_chunk_index, chunk_size=default_chunk_size):
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
        channels_ids.append(item["publisher_id"])

    if len(channels_ids) > 0:
        channels_ids = unique_array(channels_ids)
        # Merge claim ids
        build_channels_dataset(channels_ids)
        return True
    return False
