import pandas as pd
from utils import save_df_cache, load_df_cache


def get_updated_channels(df_ref, df_channels):
    df_updated = df_ref[["channel_id"]].copy()
    df_updated = pd.merge(df_channels, df_updated, on="channel_id")
    return df_updated


def merge_updated_channels(df_list):
    df_merged = (
        pd.concat(df_list)
        .drop_duplicates(subset="channel_id", keep="last")
        .reset_index(drop=True)
    )
    return df_merged


def update_channels_cache(df_ref, df_channels, cache_id):
    df_updated_channels = get_updated_channels(df_ref, df_channels)
    df_merged = df_updated_channels.copy()
    cache_name = f"{cache_id}_cache"
    # Read from local cache
    try:
        # Load cached data
        df_cache = load_df_cache(cache_name)
        # Merge new data
        df_merged = merge_updated_channels([df_cache, df_updated_channels])
    except FileNotFoundError:
        pass

    if not df_merged.empty:
        # Save to local cache
        save_df_cache(cache_name, df_merged)


def merge_updated_streams(df_list):
    df_merged = (
        pd.concat(df_list)
        .drop_duplicates(subset="stream_id", keep="last")
        .reset_index(drop=True)
    )
    return df_merged


def update_streams_cache(df_updated_streams, cache_id):
    df_merged = df_updated_streams.copy()
    cache_name = f"{cache_id}_cache"
    # Read from local cache
    try:
        # Load cached data
        cached = load_df_cache(cache_name)
        # Merge new data
        df_merged = merge_updated_streams([cached, df_merged])
    except FileNotFoundError:
        pass

    if not df_merged.empty:
        # Save to local cache
        save_df_cache(cache_name, df_merged)
