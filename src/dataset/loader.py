import re
import json
import asyncio
import numpy as np
import pandas as pd
from config import config
from lbry import filtered_outpoints
from utils import load_df_cache, get_outpoints, now_timestamp
from vocabulary import MULTILINGUAL
from status import main_status
from analysis.music import is_artist

# Load block list
with open(config["BLOCK_LIST"], "r") as f:
    BLOCK_LIST = json.load(f)

# Blocked keywords
BLOCKED_KEYWORDS = BLOCK_LIST["KEYWORDS"]

# Create dataframes from dataset chunk
class Dataset_chunk_loader:
    def __init__(self):
        self.load()

    def load(self):
        self.valid = True
        self.df_streams = self.load_streams_data()
        self.df_channels = self.load_channels_data()

    # Load streams dataframe
    def load_streams_data(self):
        df_streams = load_df_cache("stream_seeds")
        # Not enough data to process dataset chunk
        if df_streams.empty:
            self.valid = False
        else:
            df_streams = self.prepare_streams_data(df_streams)
            if df_streams.empty:
                # No relevant streams on chunk to update
                self.valid = "empty"
        return df_streams

    # Load channels dataframe
    def load_channels_data(self):
        df_channels = load_df_cache("channel_seeds")
        # Not enough data to process dataset chunk
        if df_channels.empty:
            self.valid = False
        else:
            df_channels = self.prepare_channels_data(df_channels)
            if df_channels.empty:
                # No relevant streams on chunk to update
                self.valid = "empty"
        return df_channels

    # Initial preparation of streams dataframe
    def prepare_streams_data(self, df):
        df_streams = df.copy()

        # No updated streams on chunk
        if df_streams.empty:
            return df_streams
        # Generate and append outpoints
        df_streams["outpoint"] = (
            df_streams["transaction_hash_id"] + ":" + df_streams["vout"].astype(str)
        )

        # Filter blocked content
        if filtered_outpoints and len(filtered_outpoints) > 0:
            filter_mask = ~df_streams.outpoint.isin(filtered_outpoints)
            df_streams = df_streams.loc[filter_mask]

        # Filter by update date
        current = main_status.status
        if current["init_sync"] and current["updated"]:
            filter_mask = df_streams.modified_at >= current["updated"]
            df_streams = df_streams.loc[filter_mask]

        # Remove irrelevant columns
        df_streams = df_streams.drop(
            ["transaction_hash_id", "vout", "outpoint"], axis=1
        )

        return df_streams

    def prepare_channels_data(self, df):
        df_channels = df.copy()

        if df_channels.empty:
            return df_channels

        # Generate and append outpoints
        df_channels["outpoint"] = (
            df_channels["transaction_hash_id"] + ":" + df_channels["vout"].astype(str)
        )

        # Filter blocked content
        if filtered_outpoints and len(filtered_outpoints) > 0:
            filter_mask = ~df_channels.outpoint.isin(filtered_outpoints)
            df_channels = df_channels.loc[filter_mask]

        # Remove irrelevant columns
        df_channels = df_channels.drop(
            ["transaction_hash_id", "vout", "outpoint"], axis=1
        )

        if df_channels.empty:
            return df_channels

        # Fix missing titles of safelist:
        # Use channel name as title "@channel-name" -> "channel name"
        df_channels.channel_title = (
            df_channels.channel_title.fillna("").astype(str).str.strip()
        )

        df_channels.loc[
            (is_artist(df_channels) & df_channels["channel_title"] == ""),
            "channel_title",
        ] = (
            df_channels["channel_name"].str.replace("@", "").str.replace("-", " ")
        )

        # Filter missing titles
        df_channels = df_channels[df_channels.channel_title.str.len() > 1]

        if df_channels.empty:
            return df_channels

        # Apply filters
        filter_mask = ~df_channels.channel_title.str.lower().str.contains(
            "|".join(MULTILINGUAL["AUDIOBOOK"]), case=False, na=False
        ) & ~df_channels.channel_title.str.lower().str.contains(
            "|".join(BLOCKED_KEYWORDS), case=False
        )

        df_channels = df_channels.loc[filter_mask]

        return df_channels
