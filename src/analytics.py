import time
import pandas as pd
import numpy as np
from logger import console
from lbry import get_view_count, get_likes_count


def fetch_stream_analytics(df):
    df_stream = df.copy()
    if not "view_count" in df_stream.columns:
        df_stream["view_count"] = 0
    if not "likes_count" in df_stream.columns:
        df_stream["likes_count"] = 0

    # Streams data
    stream_ids = df_stream.index.values
    streams_count = len(stream_ids)

    # Chunk data
    chunks = [stream_ids]
    chunk_index = 0
    max_chunk_size = 50

    # Split stream data on chunks
    if streams_count >= max_chunk_size:
        chunks = np.array_split(stream_ids, int(streams_count / max_chunk_size))

    for chunk in chunks:
        # logger
        chunk_index = chunk_index + 1
        console.update_status(
            f"[green] --- Syncing analytics subset chunk ~ ({len(chunks)}/{chunk_index})"
        )

        chunk_data = list(chunk)
        view_count = get_view_count(chunk_data)
        likes_count = get_likes_count(chunk_data)
        analytics_data = {"view_count": view_count, "likes_count": likes_count}
        df_analytics = pd.DataFrame(index=chunk_data, data=analytics_data)
        df_stream.loc[chunk_data, "view_count"] = view_count
        df_stream.loc[chunk_data, "likes_count"] = likes_count
        # Delay next api calls
        time.sleep(1)

    console.info(
        "SYNC",
        f"Dataset",
        action="Synced analytics!",
        stop_status=True,
    )

    return df_stream
