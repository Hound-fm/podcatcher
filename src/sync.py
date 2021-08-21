import numpy as np
import pandas as pd
from lbry import lbry_proxy
from utils import load_df_cache
from logger import console
from constants import STREAM_TYPES, CHANNEL_TYPES
from elastic import Elastic
from elastic.definitions import (
    INDEX,
    INDICES,
    FIELDS_STREAM_AUTOCOMPLETE,
    FIELDS_CHANNEL_AUTOCOMPLETE,
    MAPPINGS_STREAM_AUTOCOMPLETE,
    MAPPINGS_CHANNEL_AUTOCOMPLETE,
)


def sync_autocomplete_indices():
    elastic = Elastic()
    for index in INDICES:
        if index == INDEX["CHANNEL"]:
            for channel_type in CHANNEL_TYPES:
                source = {
                    "index": index,
                    "_source": FIELDS_CHANNEL_AUTOCOMPLETE,
                    "query": {
                        "constant_score": {
                            "filter": {"term": {"channel_type": channel_type}},
                        },
                    },
                }
                elastic.generate_autocomple_index(
                    channel_type,
                    source,
                    MAPPINGS_CHANNEL_AUTOCOMPLETE,
                )
        if index == INDEX["STREAM"]:
            for stream_type in STREAM_TYPES:
                source = {
                    "index": index,
                    "_source": FIELDS_STREAM_AUTOCOMPLETE,
                    "query": {
                        "constant_score": {
                            "filter": {"term": {"stream_type": stream_type}}
                        },
                    },
                }
                elastic.generate_autocomple_index(
                    stream_type,
                    source,
                    MAPPINGS_STREAM_AUTOCOMPLETE,
                )


# Sync cache data to elasticsearch
def sync_cache_indices():
    elastic = Elastic()
    for index in INDICES:
        df_cache = load_df_cache(f"{index}_cache")
        elastic.append_df_chunk(index, df_cache)


def sync_elastic_search():
    # Run elasticsearch "sync" tasks
    sync_cache_indices()
    sync_autocomplete_indices()


# Load unavailable data of streams from sdk
def sync_channels_metadata(channels_ids, channels_metadata={}):
    df_results = pd.DataFrame()

    # No channels or metadata to sync
    if not (len(channels_ids) or len(channels_metadata)):
        return df_results

    # Columns
    # Normal python list
    tags = []
    languages = []
    # Numpy arrays
    ids = np.array([], dtype=np.str_)
    status = np.array([], dtype=np.str_)
    trending = np.array([], dtype=np.float32)
    thumbnails = np.array([], dtype=np.str_)
    creation_dates = np.array([], dtype=np.int32)

    # Sdk api request
    for channel_id in channels_ids:
        # Claim metadata
        if channel_id in channels_metadata:
            metadata = channels_metadata[channel_id]
            # default values
            thumbnail = ""
            claim_tags = []
            claim_status = "spent"
            claim_trending = 0
            claim_languages = []
            creation_date = 0
            # Get claim status
            if "claim_op" in metadata:
                claim_status = "active"

            if not claim_status or claim_status != "active":
                continue

            # Used as fallback for "release_date"
            if "timestamp" in metadata:
                creation_date = metadata["timestamp"]

            # Get claim stats
            if "meta" in metadata:
                meta = metadata["meta"]
                if "trending_mixed" in meta:
                    claim_trending = meta["trending_mixed"]
                if "creation_timestamp" in meta:
                    creation_date = meta["creation_timestamp"]

            # Get claim value metadata
            if "value" in metadata:
                value = metadata["value"]
                if "languages" in value:
                    claim_languages = value["languages"]
                if "tags" in value:
                    claim_tags = set(value["tags"])
                if "thumbnail" in value:
                    if "url" in value["thumbnail"]:
                        thumbnail = value["thumbnail"]["url"]
            # Fill columns values
            ids = np.append(ids, channel_id)
            status = np.append(status, claim_status)
            trending = np.append(trending, claim_trending)
            thumbnails = np.append(thumbnails, thumbnail)
            creation_dates = np.append(creation_dates, creation_date)
            # Use normal python list for nested lists
            tags.append(claim_tags)
            languages.append(claim_languages)

    # Append id
    df_results["channel_id"] = ids
    # Append columns
    df_results["tags"] = tags
    df_results["status"] = status
    df_results["trending"] = trending
    df_results["languages"] = languages
    df_results["thumbnail"] = thumbnails
    df_results["creation_date"] = pd.to_datetime(creation_dates, utc=True, unit="s")

    # Return new dataframe
    return df_results


# Load unavailable data of claims from sdk
def sync_claims_metadata(streams_urls, channels_ids):
    df_streams_metadata = pd.DataFrame()

    # Store channel metadata
    channels_metadata = {}

    # Columns
    # Normal python list
    tags = []
    languages = []
    # Numpy arrays
    ids = np.array([], dtype=np.str_)
    status = np.array([], dtype=np.str_)
    licenses = np.array([], dtype=np.str_)
    reposted = np.array([], dtype=np.int8)
    trending = np.array([], dtype=np.float32)
    thumbnails = np.array([], dtype=np.str_)
    release_dates = np.array([], dtype=np.int32)
    fee_amount = np.array([], dtype=np.float64)
    fee_currency = np.array([], dtype=np.str_)

    # Sdk api request
    payload = {"urls": streams_urls}
    res = lbry_proxy("resolve", payload)
    if res:
        if "error" in res:
            if "message" in res["error"]:
                console.error("LBRY_SDK", f"{res['error']['message']}")
            return {"streams": pd.DataFrame(), "channels": pd.DataFrame()}
        if "result" in res:
            res = res["result"]
    else:
        return {"streams": pd.DataFrame(), "channels": pd.DataFrame()}

    for url in streams_urls:
        if url in res:
            metadata = res[url]
            # default values
            thumbnail = ""
            claim_id = ""
            claim_tags = []
            release_date = 0
            claim_status = "spent"
            claim_license = ""
            claim_reposted = 0
            claim_trending = 0
            claim_languages = []
            # Fee data
            claim_fee = {
                "amount": 0.0,
                "currency": "LBC",
            }
            # Get claim status
            if "claim_op" in metadata:
                if "is_channel_signature_valid" in metadata:
                    if metadata["is_channel_signature_valid"]:
                        channel = metadata["signing_channel"]
                        claim_status = "active"
                        if "claim_id" in channel:
                            channels_metadata[channel["claim_id"]] = channel

            # Invalid stream. Skip metadata
            if not claim_status or claim_status != "active":
                continue

            if "claim_id" in metadata:
                claim_id = metadata["claim_id"]

            # Used as fallback for "release_date"
            if "timestamp" in metadata:
                release_date = metadata["timestamp"]

            # Get claim stats
            if "meta" in metadata:
                meta = metadata["meta"]
                if "reposted" in meta:
                    claim_reposted = meta["reposted"]
                if "trending_mixed" in meta:
                    claim_trending = meta["trending_mixed"]
                # Used as fallback for "release_date"
                if "creation_timestamp" in meta:
                    release_date = meta["creation_timestamp"]

            # Get claim value metadata
            if "value" in metadata:
                value = metadata["value"]
                if "fee" in value:
                    if "amount":
                        claim_fee["amount"] = value["fee"]["amount"]
                    if "currency":
                        claim_fee["currency"] = value["fee"]["currency"]
                if "license" in value:
                    license = value["license"].lower()
                    if (len(license) > 0) and (license != "none"):
                        claim_license = value["license"]
                if "languages" in value:
                    claim_languages = value["languages"]
                if "tags" in value:
                    claim_tags = set(value["tags"])
                if "thumbnail" in value:
                    if "url" in value["thumbnail"]:
                        thumbnail = value["thumbnail"]["url"]
                # SDK returns release_date as string instead of int
                # We need to convert it first:
                if "release_time" in value:
                    release_date = int(value["release_time"])

            # Fill columns values
            ids = np.append(ids, claim_id)
            status = np.append(status, claim_status)
            licenses = np.append(licenses, claim_license)
            reposted = np.append(reposted, claim_reposted)
            trending = np.append(trending, claim_trending)
            thumbnails = np.append(thumbnails, thumbnail)
            fee_amount = np.append(fee_amount, claim_fee["amount"])
            fee_currency = np.append(fee_currency, claim_fee["currency"])
            release_dates = np.append(release_dates, release_date)
            # Use normal python list for nested lists
            tags.append(claim_tags)
            languages.append(claim_languages)

    # Append stream metadata columns
    df_streams_metadata["stream_id"] = ids
    df_streams_metadata["tags"] = tags
    df_streams_metadata["status"] = status
    df_streams_metadata["license"] = licenses
    df_streams_metadata["reposted"] = reposted
    df_streams_metadata["trending"] = trending
    df_streams_metadata["languages"] = languages
    df_streams_metadata["thumbnail"] = thumbnails
    df_streams_metadata["fee_amount"] = fee_amount
    df_streams_metadata["fee_currency"] = fee_currency

    # Fix data types:
    df_streams_metadata["fee_amount"] = df_streams_metadata["fee_amount"].astype(
        np.float64
    )
    df_streams_metadata["release_date"] = pd.to_datetime(
        release_dates, utc=True, unit="s"
    )

    # Append channels metadata columns
    df_channels_metada = sync_channels_metadata(channels_ids, channels_metadata)

    # Return new dataframe
    return {"streams": df_streams_metadata, "channels": df_channels_metada}


def sync_metadata(df_ref_streams, df_ref_channels, max_chunk_size=100):
    # Dataframes
    df_streams_metadata = pd.DataFrame()
    df_channels_metadata = pd.DataFrame()
    streams_urls = df_ref_streams["url"].unique()
    channels_ids = df_ref_channels["channel_id"].unique()
    # Initial chunk
    chunks = [streams_urls]
    chunk_index = 0

    # No data to sync
    if not (len(streams_urls) or len(channels_ids)):
        return {"streams": df_streams_metadata, "channels": df_channels_metada}

    total_urls = len(streams_urls)

    if total_urls >= max_chunk_size:
        chunks = np.array_split(streams_urls, int(total_urls / max_chunk_size))

    for chunk in chunks:
        chunk_urls = list(chunk)
        chunk_index += 1

        if not len(chunk_urls):
            continue

        console.update_status(
            f"[green] --- Syncing metadata subset chunk ~ ({len(chunks)}/{chunk_index})"
        )

        chunk_metadata = sync_claims_metadata(chunk_urls, channels_ids)
        if not chunk_metadata["streams"].empty:
            if df_streams_metadata.empty:
                df_streams_metadata = chunk_metadata["streams"]
            else:
                df_streams_metadata = (
                    pd.concat([df_streams_metadata, chunk_metadata["streams"]])
                    .drop_duplicates(subset="stream_id", keep="last")
                    .reset_index(drop=True)
                )
        if not chunk_metadata["channels"].empty:
            if df_channels_metadata.empty:
                df_channels_metadata = chunk_metadata["channels"]
            else:
                df_channels_metadata = (
                    pd.concat([df_channels_metadata, chunk_metadata["channels"]])
                    .drop_duplicates(subset="channel_id", keep="last")
                    .reset_index(drop=True)
                )

    # Return new dataframe
    return {"streams": df_streams_metadata, "channels": df_channels_metadata}
