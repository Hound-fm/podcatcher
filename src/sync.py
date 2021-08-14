import numpy as np
import pandas as pd
from elastic import Elastic
from lbry import lbry_proxy
from utils import load_df_cache
from logger import console
from constants import ELASTIC_INDICES, ELASTIC_INDEX

# Sync cache data to elasticsearch
def sync_elastic_search():
    elastic = Elastic()
    for index in ELASTIC_INDICES:
        df_cache = load_df_cache(f"df_{index}")
        if not df_cache.empty:
            chunk_type = "stream"
            if (index == ELASTIC_INDEX["ARTISTS"]) or (
                index == ELASTIC_INDEX["PODCAST_SERIES"]
            ):
                chunk_type = "channel"
            elastic.append_df_chunk(index, df_cache, chunk_type)


# Load unavailable data of streams from sdk
def sync_channels_metadata(channels_ids, channels_metadata={}):
    df_results = pd.DataFrame()

    # No channels or metadata to sync
    if not (len(channels_ids) or len(channels_metadata)):
        return df_results

    # Columns
    ids = []
    tags = []
    status = []
    trending = []
    languages = []
    thumbnails = []
    creation_times = []

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
            creation_time = 0
            # Get claim status
            if "claim_op" in metadata:
                claim_status = "active"

            if not claim_status or claim_status != "active":
                continue

            # Used as fallback for "creation_timestamp"
            if "timestamp" in metadata:
                creation_time = metadata["timestamp"]

            # Get claim stats
            if "meta" in metadata:
                meta = metadata["meta"]
                if "trending_mixed" in meta:
                    claim_trending = meta["trending_mixed"]
                if "creation_timestamp" in meta:
                    creation_time = meta["creation_timestamp"]

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
            ids.append(channel_id)
            tags.append(claim_tags)
            status.append(claim_status)
            trending.append(claim_trending)
            languages.append(claim_languages)
            thumbnails.append(thumbnail)
            creation_times.append(creation_time)

    # Append id
    df_results["channel_id"] = ids
    # Append columns
    df_results["tags"] = tags
    df_results["status"] = status
    df_results["trending"] = trending
    df_results["languages"] = languages
    df_results["thumbnail"] = thumbnails
    df_results["creation_time"] = creation_times

    # Return new dataframe
    return df_results


# Load unavailable data of claims from sdk
def sync_claims_metadata(streams_urls, channels_ids):
    df_streams_metadata = pd.DataFrame()

    # Columns
    ids = []
    tags = []
    status = []
    licenses = []
    reposted = []
    trending = []
    languages = []
    thumbnails = []
    fee_amount = []
    fee_currency = []
    release_times = []

    # Store channel metadata
    channels_metadata = {}

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
            claim_id = None
            claim_tags = []
            release_time = 0
            claim_status = "spent"
            claim_license = None
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

            # Used as fallback for "release_time"
            if "timestamp" in metadata:
                release_time = metadata["timestamp"]

            # Get claim stats
            if "meta" in metadata:
                meta = metadata["meta"]
                if "reposted" in meta:
                    claim_reposted = meta["reposted"]
                if "trending_mixed" in meta:
                    claim_trending = meta["trending_mixed"]
                # Used as fallback for "release_time"
                if "creation_timestamp" in meta:
                    release_time = meta["creation_timestamp"]

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
                # SDK returns release_time as string instead of int
                # We need to convert it first:
                if "release_time" in value:
                    release_time = int(value["release_time"])

            # Append id
            ids.append(claim_id)
            # Fill columns values
            tags.append(claim_tags)
            status.append(claim_status)
            licenses.append(claim_license)
            reposted.append(claim_reposted)
            trending.append(claim_trending)
            languages.append(claim_languages)
            thumbnails.append(thumbnail)
            release_times.append(release_time)
            # Append fee data
            fee_currency.append(claim_fee["currency"])
            fee_amount.append(claim_fee["amount"])

    # Append stream metadata columns
    df_streams_metadata["stream_id"] = ids
    df_streams_metadata["tags"] = tags
    df_streams_metadata["status"] = status
    df_streams_metadata["license"] = licenses
    df_streams_metadata["reposted"] = reposted
    df_streams_metadata["trending"] = trending
    df_streams_metadata["languages"] = languages
    df_streams_metadata["thumbnail"] = thumbnails
    df_streams_metadata["release_time"] = release_times
    df_streams_metadata["fee_amount"] = fee_amount
    df_streams_metadata["fee_currency"] = fee_currency

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
