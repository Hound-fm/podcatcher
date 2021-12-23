import numpy as np
import pandas as pd
from lbry import lbry_proxy, filtered_outpoints
from utils import load_df_cache
from logger import console
import collections
from collection import COLLECTIONS
from constants import STREAM_TYPES, CHANNEL_TYPES
from elastic import Elastic
from elastic.definitions import INDEX, INDICES


# Sync cache data to elasticsearch
def sync_cache_indices():
    elastic = Elastic()
    for index in INDICES:
        df_cache = load_df_cache(f"{index}_cache")
        if not df_cache.empty:
            elastic.append_df_chunk(index, df_cache)


def sync_elastic_search():
    # Run elasticsearch "sync" tasks
    sync_cache_indices()


def get_collection_title(collection_key):
    return collection_key.replace("-", " ").title()


def update_collection(collection_id, collection_list):
    res = lbry_proxy(
        "collection_update",
        {
            "claim_id": collection_id,
            "claims": collection_list,
            "clear_claims": True,
            "channel_id": "2a1a0936017f6677628aff2b90c6675569d0d07c",
        },
    )
    if res:
        if "error" in res:
            if "message" in res["error"]:
                console.error("LBRY_SDK", f"{res['error']['message']}")
        if "result" in res:
            console.info("LBRY_SDK", f"Collection updated: {collection_id}")


def create_collection(collection_key, collection_list, collection_description):
    res = lbry_proxy(
        "collection_create",
        {
            "bid": "0.001",
            "name": collection_key,
            "title": get_collection_title(collection_key),
            "claims": collection_list,
            "channel_id": "2a1a0936017f6677628aff2b90c6675569d0d07c",
            "description": collection_description,
        },
    )
    if res:
        if "error" in res:
            if "message" in res["error"]:
                console.error("LBRY_SDK", f"{res['error']['message']}")
        if "result" in res:
            console.info("LBRY_SDK", f"Collection created: {collection_key}")


def sync_collections_metadata():
    # Sdk api request
    res = lbry_proxy("collection_list", {})
    if res:
        if "error" in res:
            if "message" in res["error"]:
                console.error("LBRY_SDK", f"{res['error']['message']}")
        if "result" in res and "items" in res["result"]:
            current_collections = res["result"]["items"]

            for collection_key in COLLECTIONS:
                exists = False
                # Check if collection exists:
                for collection in current_collections:
                    if collection["normalized_name"] == collection_key:
                        exists = True
                        # Check if collection needs update
                        if collections.Counter(
                            collection["value"]["claims"]
                        ) != collections.Counter(COLLECTIONS[collection_key]["claims"]):
                            update_collection(
                                collection["claim_id"],
                                COLLECTIONS[collection_key]["claims"],
                            )
                            break
                if not exists:
                    # Create collection
                    create_collection(
                        collection_key,
                        COLLECTIONS[collection_key]["claims"],
                        COLLECTIONS[collection_key]["description"],
                    )

    else:
        console.error("LBRY_SDK", f"Uknown error")


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
    creation_dates = []
    # Numpy arrays
    ids = np.array([], dtype=np.str_)
    status = np.array([], dtype=np.str_)
    thumbnails = np.array([], dtype=np.str_)

    # Sdk api request
    for channel_id in channels_ids:
        # Claim metadata
        if channel_id in channels_metadata:
            metadata = channels_metadata[channel_id]
            # default values
            thumbnail = ""
            claim_tags = []
            claim_status = "spent"
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
            thumbnails = np.append(thumbnails, thumbnail)
            # Use normal python list for nested lists
            tags.append(claim_tags)
            languages.append(claim_languages)
            creation_dates.append(creation_date)

    # Append id
    df_results["channel_id"] = ids
    # Append columns
    df_results["tags"] = tags
    df_results["status"] = status
    df_results["languages"] = languages
    df_results["thumbnail"] = thumbnails
    df_results["creation_date"] = creation_dates

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
    release_dates = []
    # Numpy arrays
    ids = np.array([], dtype=np.str_)
    status = np.array([], dtype=np.str_)
    licenses = np.array([], dtype=np.str_)
    reposted = np.array([], dtype=np.int8)
    thumbnails = np.array([], dtype=np.str_)
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
            thumbnails = np.append(thumbnails, thumbnail)
            fee_amount = np.append(fee_amount, claim_fee["amount"])
            fee_currency = np.append(fee_currency, claim_fee["currency"])
            # Use normal python list for nested lists
            tags.append(claim_tags)
            languages.append(claim_languages)
            release_dates.append(release_date)

    # Append stream metadata columns
    df_streams_metadata["stream_id"] = ids
    df_streams_metadata["tags"] = tags
    df_streams_metadata["status"] = status
    df_streams_metadata["license"] = licenses
    df_streams_metadata["reposted"] = reposted
    df_streams_metadata["languages"] = languages
    df_streams_metadata["thumbnail"] = thumbnails
    df_streams_metadata["fee_amount"] = fee_amount
    df_streams_metadata["fee_currency"] = fee_currency
    df_streams_metadata["release_date"] = release_dates

    # Fix data types:
    df_streams_metadata["fee_amount"] = df_streams_metadata["fee_amount"].astype(
        np.float64
    )

    # Append channels metadata columns
    df_channels_metada = sync_channels_metadata(channels_ids, channels_metadata)

    # Return new dataframe
    return {"streams": df_streams_metadata, "channels": df_channels_metada}


def sync_metadata(df_ref_streams, df_ref_channels, max_chunk_size=90):
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

    if total_urls > max_chunk_size:
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


def get_thumbnail(item):
    if "thumbnail" in item and "url" in item["thumbnail"]:
        return item["thumbnail"]["url"]
    else:
        return ""


def get_title(item):
    if "title" in item:
        return item["title"]
    return ""


def get_effective_amount(item):
    if "effective_amount" in item:
        return item["effective_amount"]


def sync_author_metadata(chunk_ids, page=1):
    # Sdk api request
    payload = {"claim_ids": chunk_ids, "page_size": 50, "page": page}
    res = lbry_proxy("claim_search", payload)
    df_authors_chunk = pd.DataFrame()

    if res and "result" in res and "items" in res["result"]:
        df_authors_chunk = pd.DataFrame.from_records(res["result"]["items"])
        df_authors_chunk["author_id"] = df_authors_chunk["claim_id"]
        df_authors_chunk["author_url"] = df_authors_chunk["canonical_url"].str.replace(
            "lbry://", ""
        )
        df_authors_chunk["author_url"] = df_authors_chunk["author_url"].str.replace(
            "#", ":"
        )
        df_authors_chunk["author_name"] = df_authors_chunk["name"]

        df_authors_chunk["author_title"] = df_authors_chunk["value"].apply(get_title)
        df_authors_chunk.loc[
            df_authors_chunk["author_title"] == "", "author_title"
        ] = df_authors_chunk["author_name"].str.replace("@", "")
        df_authors_chunk["author_thumbnail"] = df_authors_chunk["value"].apply(
            get_thumbnail
        )
        df_authors_chunk["outpoint"] = (
            df_authors_chunk["txid"] + ":" + df_authors_chunk["nout"].astype(str)
        )

        df_authors_chunk["effective_amount"] = df_authors_chunk["meta"].apply(
            get_effective_amount
        )
        df_authors_chunk["effective_amount"] = pd.to_numeric(
            df_authors_chunk["effective_amount"]
        )

        # Channel tier filter:
        # The value will increase as spam increases (Fake channels, bots, etc..)
        df_authors_chunk = df_authors_chunk.loc[
            df_authors_chunk.effective_amount >= 0.1
        ]

        # Active channels
        df_authors_chunk = df_authors_chunk[df_authors_chunk["claim_op"].notnull()]

        # Filter blocked channels
        if filtered_outpoints and len(filtered_outpoints) > 0:
            filter_mask = ~df_authors_chunk.outpoint.isin(filtered_outpoints)
            df_authors_chunk = df_authors_chunk.loc[filter_mask]

        # Extract relevant author data
        df_authors_chunk = df_authors_chunk[
            [
                "author_id",
                "author_url",
                "author_name",
                "author_title",
                # "author_thumbnail",
            ]
        ]
        return {
            "chunk": df_authors_chunk,
            "total_pages": res["result"].get("total_pages", 0),
        }
    # Return empty data
    return {
        "chunk": df_authors_chunk,
        "total_pages": 0,
    }


# repost / tip / support author
def sync_authors(claim_ids, max_chunk_size=30):
    # Initial dataframe
    df_authors = pd.DataFrame()
    # Initial chunk
    chunks = [claim_ids]
    chunk_index = 0

    # No data to sync
    total_ids = len(claim_ids)

    if total_ids > max_chunk_size:
        chunks = np.array_split(claim_ids, int(total_ids / max_chunk_size))

    for chunk in chunks:
        chunk_ids = list(chunk)
        chunk_index += 1
        if not len(chunk_ids):
            continue
        console.update_status(
            f"[green] --- Syncing metadata subset chunk ~ ({len(chunks)}/{chunk_index})"
        )
        metadata_chunk = sync_author_metadata(chunk_ids, 1)
        df_authors_chunk = metadata_chunk["chunk"]

        if not df_authors_chunk.empty:
            if df_authors.empty:
                df_authors = df_authors_chunk
            else:
                df_authors = pd.concat(
                    [df_authors, df_authors_chunk], ignore_index=True
                )

        # Fetch next page
        if not df_authors.empty and metadata_chunk["total_pages"] > 1:
            for sub_chunk_index in range(2, metadata_chunk["total_pages"] + 1):
                next_metadata_chunk = sync_author_metadata(chunk_ids, sub_chunk_index)
                next_df_authors_chunk = next_metadata_chunk["chunk"]
                if not next_df_authors_chunk.empty:
                    df_authors = pd.concat(
                        [df_authors, next_df_authors_chunk], ignore_index=True
                    )

    return df_authors
