# Build json datasets from chainquery results
import time
import asyncio
from utils import unique_array, write_json_file, get_current_time
from chainquery import query, queries

default_chunk_size = 100
default_chunk_index = 0


async def update_build_status():
    status = {
        "version": "0.0.1",
        "init_sync": get_current_time(),
    }
    write_json_file(status, "status.json")


# Build tags dataset
async def build_tags_dataset(id_list):
    results = query(queries.bulk_fetch_tags(id_list))
    write_json_file(results, "tags.json")


# Build channles dataset
async def build_channels_dataset(id_list):
    results = query(queries.bulk_fetch_channels(id_list))
    write_json_file(results, "channels.json")


# Build stream dataset
async def build_streams_dataset(
    chunk_index=default_chunk_index,
    chunk_size=default_chunk_size,
):
    query_options = {"limit": chunk_size, "offset": chunk_size * chunk_index}
    results = query(queries.bulk_fetch_streams(), query_options)
    write_json_file(results, "streams.json")
    return results


# Build data sets
async def build_dataset_chunk(
    chunk_index=default_chunk_index, chunk_size=default_chunk_size
):
    streams_ids = []
    channels_ids = []

    # Get content
    content = await build_streams_dataset(chunk_size)
    content_size = len(content) if content else 0
    # Stop function if there is no content
    if content_size == 0:
        return

    # Get list of ids
    for item in content:
        streams_ids.append(item["claim_id"])
        channels_ids.append(item["publisher_id"])

    if len(channels_ids) > 0:
        # Remove duplicated channels
        channels_ids = unique_array(channels_ids)
        # Merge claim ids
        merged_ids = list(set(streams_ids + channels_ids))
        build_tasks = [
            # Tags chunk
            build_tags_dataset(merged_ids),
            # Channels chunk
            build_channels_dataset(channels_ids),
        ]
        await asyncio.gather(*build_tasks)
