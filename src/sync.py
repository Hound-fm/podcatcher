import pandas as pd
from elastic import Elastic
from lbry import lbry_proxy
from utils import load_df_cache
from logger import log
from constants import ELASTIC_INDICES, ELASTIC_INDEX

# Sync cache data to elasticsearch
def sync_elastic_search():
    elastic = Elastic()
    for index in ELASTIC_INDICES:
        df_cache = load_df_cache(f"df_{index}")
        chunk_type = "stream"
        if (
            index == ELASTIC_INDEX["ARTISTS"]
            or index == ELASTIC_INDEX["PODCAST_SERIES"]
        ):
            chunk_type = "channel"
        elastic.append_df_chunk(index, df_cache, chunk_type)


# Load unavailable data of streams from sdk
def sync_channels_data(df):
    # New dataframe
    df_results = df.copy()

    # Columns
    tags = []
    status = []
    trending = []
    languages = []
    perm_urls = []
    thumbnails = []

    # Sdk api request
    urls = df_results["cannonical_url"].tolist()
    payload = {"urls": urls}
    res = lbry_proxy("resolve", payload)
    if res:
        if "error" in res:
            log.error(f'Error: {res["error"]["message"]}')
            return pd.DataFrame()
        if "result" in res:
            res = res["result"]
    else:
        return pd.DataFrame()

    for url in urls:
        # Claim metadata
        metadata = res[url]
        # default values
        thumbnail = ""
        claim_tags = []
        claim_status = "spent"
        claim_trending = 0
        claim_perm_url = None
        claim_languages = []
        claim_perm_url = None

        # Get claim status
        if "claim_op" in metadata:
            claim_status = "active"

        # Get claim stats
        if "meta" in metadata:
            meta = metadata["meta"]
            if "trending_mixed" in meta:
                claim_trending = meta["trending_mixed"]

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

        # Get claim url
        if "permanent_url" in metadata:
            claim_perm_url = metadata["permanent_url"]

        # Fill columns values
        tags.append(claim_tags)
        status.append(claim_status)
        trending.append(claim_trending)
        languages.append(claim_languages)
        perm_urls.append(claim_perm_url)
        thumbnails.append(thumbnail)

    # Append columns
    df_results["tags"] = tags
    df_results["status"] = status
    df_results["trending"] = trending
    df_results["languages"] = languages
    df_results["perm_url"] = perm_urls
    df_results["thumbnail"] = thumbnails

    # Return new dataframe
    return df_results


# Load unavailable data of streams from sdk
def sync_streams_data(df):
    # New dataframe
    df_results = df.copy()

    # Columns
    tags = []
    status = []
    licenses = []
    reposted = []
    trending = []
    languages = []
    perm_urls = []
    thumbnails = []

    # Sdk api request
    urls = df_results["cannonical_url"].tolist()
    payload = {"urls": urls}
    res = lbry_proxy("resolve", payload)
    if res:
        if "error" in res:
            log.error(f'Error: {res["error"]["message"]}')
            return pd.DataFrame()
        if "result" in res:
            res = res["result"]
    else:
        return pd.DataFrame()

    for url in urls:
        # Claim metadata
        metadata = res[url]
        # default values
        thumbnail = ""
        claim_tags = []
        claim_status = "spent"
        claim_license = None
        claim_reposted = 0
        claim_trending = 0
        claim_perm_url = None
        claim_languages = []
        claim_perm_url = None

        # Get claim status
        if "claim_op" in metadata:
            if "is_channel_signature_valid" in metadata:
                if metadata["is_channel_signature_valid"]:
                    claim_status = "active"

        # Get claim stats
        if "meta" in metadata:
            meta = metadata["meta"]
            if "reposted" in meta:
                claim_reposted = meta["reposted"]
            if "trending_mixed" in meta:
                claim_trending = meta["trending_mixed"]

        # Get claim value metadata
        if "value" in metadata:
            value = metadata["value"]
            if "license" in value:
                license = value["license"].lower()
                if (len(license) > 3) and (license != "none"):
                    claim_license = value["license"]
            if "languages" in value:
                claim_languages = value["languages"]
            if "tags" in value:
                claim_tags = set(value["tags"])
            if "thumbnail" in value:
                if "url" in value["thumbnail"]:
                    thumbnail = value["thumbnail"]["url"]

        # Get claim url
        if "permanent_url" in metadata:
            claim_perm_url = metadata["permanent_url"]

        # Fill columns values
        tags.append(claim_tags)
        status.append(claim_status)
        licenses.append(claim_license)
        reposted.append(claim_reposted)
        trending.append(claim_trending)
        languages.append(claim_languages)
        perm_urls.append(claim_perm_url)
        thumbnails.append(thumbnail)

    # Append columns
    df_results["tags"] = tags
    df_results["status"] = status
    df_results["license"] = licenses
    df_results["reposted"] = reposted
    df_results["trending"] = trending
    df_results["languages"] = languages
    df_results["perm_url"] = perm_urls
    df_results["thumbnail"] = thumbnails

    # Return new dataframe
    return df_results
