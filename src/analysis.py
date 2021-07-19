import time
import asyncio
import numpy as np
import pandas as pd
from lbry import lbry_proxy
from dataset import build_dataset_chunk
from dataset.loader import Dataset_chunk_loader
from constants import STREAM_TYPE, STREAM_TYPES, FILTER_TAGS
from vocabulary import GENRES, MULTILINGUAL
from utils import unique_clean_list

# Global values
dataset_chunk_index = -1
dataset_chunk_size = 1000
delay = 24

# Scan all existent claims
def full_scan():
    global dataset_chunk_size
    global dataset_chunk_index
    dataset_chunk_index += 1
    ready = build_dataset_chunk(dataset_chunk_index, dataset_chunk_size)

    if ready:
        # Process dataset chunk
        success = process_dataset_chunk()
        # Dataset chunk was processed successfully
        if success:
            print("chunk: ", dataset_chunk_index)
            # Delay for timeout errors
            time.sleep(delay)
            # Load next dataset chunk
            full_scan()
        else:
            # Handle error
            print("Failed to process chunk ", dataset_chunk_index)


# Select dominant stream type
def select_dominant_category(categories):
    c = pd.Categorical(categories, ordered=True, categories=STREAM_TYPES)
    return c.max()


# Select known genres
def select_dominant_genres(df_tags):
    conditions = [
        # This tag defines the audiobook genre
        df_tags["stream_type"] == STREAM_TYPE["AUDIOBOOK"],
        # This tag defines the podcast genre
        df_tags["stream_type"] == STREAM_TYPE["PODCAST_EPISODE"],
        # This tag defines the music genre
        df_tags["stream_type"] == STREAM_TYPE["MUSIC_RECORDING"],
    ]

    # Select dominant column
    choices = [
        df_tags["audiobook_genre"],
        df_tags["podcast_genre"],
        df_tags["music_genre"],
    ]

    all_genres = (
        df_tags["music_genre"] + df_tags["podcast_genre"] + df_tags["audiobook_genre"]
    )

    # Return series
    return np.select(conditions, choicelist=choices, default=all_genres)


def process_tags(df):
    df_tags = df[["tags", "claim_id"]].explode("tags")
    df_tags = df_tags.rename(columns={"tags": "tag_name"})
    # Format tag name
    df_tags["tag_name"] = df_tags["tag_name"].astype(str)
    df_tags["tag_name"] = df_tags["tag_name"].str.lower()
    df_tags["tag_name"] = df_tags["tag_name"].str.strip()
    df_tags["tag_name"] = df_tags["tag_name"].str.replace("-and-", " and ")
    # Filter tags
    df_tags = df_tags.loc[~df_tags["tag_name"].isin(FILTER_TAGS)]
    return process_special_tags(df_tags)


def process_special_tags(df):
    df_tags = df.copy()
    # Find multilingual or alias version and correct tag_names:
    df_tags.loc[
        df_tags["tag_name"].str.contains("|".join(MULTILINGUAL["music"]), case=False),
        "tag_name",
    ] = STREAM_TYPE["MUSIC_RECORDING"]

    df_tags.loc[
        df_tags["tag_name"].str.contains("|".join(MULTILINGUAL["podcast"]), case=False),
        "tag_name",
    ] = STREAM_TYPE["PODCAST_EPISODE"]

    df_tags.loc[
        df_tags["tag_name"].isin(MULTILINGUAL["podcast"]), "tag_name"
    ] = STREAM_TYPE["PODCAST_EPISODE"]

    df_tags.loc[
        df_tags["tag_name"].str.contains(
            "|".join(MULTILINGUAL["audiobook"]), case=False
        ),
        "tag_name",
    ] = STREAM_TYPE["AUDIOBOOK"]

    conditions = [
        # This tag defines the category type
        df_tags["tag_name"].isin(STREAM_TYPES),
        # This tag defines the music genre
        df_tags["tag_name"].isin(GENRES["MUSIC"]),
        # This tag defines the podcast genre
        df_tags["tag_name"].isin(GENRES["PODCAST"]),
        # This tag defines the audiobook genre
        df_tags["tag_name"].isin(GENRES["AUDIOBOOK"]),
    ]

    genres = [
        "music_genre",
        "podcast_genre",
        "audiobook_genre",
    ]
    # Tags categories
    outputs = [
        "stream_type",
        *genres,
    ]

    # Find tag categories
    df_tags["category"] = np.select(conditions, outputs, "tags")

    # Pivot data
    df_tags = df_tags.reset_index(drop=True)
    df_pivot = df_tags.pivot(columns="category", values="tag_name")

    # Add missing columns
    if not "tags" in df_pivot.columns:
        df_pivot["tags"] = np.nan

    for genre in genres:
        if not genre in df_pivot.columns:
            df_pivot[genre] = np.nan

    df_tags = df_tags.merge(df_pivot, left_index=True, right_index=True)

    # Group tags by category for each claim
    df_tags = (
        df_tags.groupby("claim_id")
        .agg(
            stream_type=("stream_type", select_dominant_category),
            tags=("tags", unique_clean_list),
            # Generes
            music_genre=("music_genre", unique_clean_list),
            podcast_genre=("podcast_genre", unique_clean_list),
            audiobook_genre=("audiobook_genre", unique_clean_list),
        )
        .reset_index()
    )

    df_tags["stream_type"] = df_tags["stream_type"].astype("category")
    df_tags["genres"] = select_dominant_genres(df_tags)

    # Return relevant columns
    return df_tags[["claim_id", "stream_type", "genres", "tags"]]


def process_dataset_chunk():
    # Load dataset chunk
    chunk = Dataset_chunk_loader()
    # Not enough data to process chunk
    if not chunk.valid:
        return False
    # Merge channel data
    chunk.df_streams = pd.merge(
        chunk.df_streams,
        chunk.df_channels[["publisher_id", "publisher_name", "publisher_title"]],
        on="publisher_id",
    )
    # Get cannonical_url
    chunk.df_streams["cannonical_url"] = get_cannonical_url(chunk.df_streams)
    # Get updated data from sdk
    chunk.df_streams = sync_stream_data(chunk.df_streams)
    # SDK failed to sync data
    if chunk.df_streams.empty:
        return False
    # Filter spent / inactive claims
    chunk.df_streams = chunk.df_streams.loc[chunk.df_streams["status"] == "active"]
    # Process stream tags
    df_tags = process_tags(chunk.df_streams)
    # Merge tags data
    chunk.df_streams = pd.merge(chunk.df_streams, df_tags, on="claim_id")
    # Filter uknown types
    chunk.df_streams = chunk.df_streams.loc[chunk.df_streams["stream_type"].notnull()]
    # Podcasts
    podcasts = chunk.df_streams.loc[
        chunk.df_streams["stream_type"] == STREAM_TYPE["PODCAST_EPISODE"]
    ]
    return True


def get_cannonical_url(df):
    df_claims = df.copy()
    df_claims = df_claims[["claim_id", "name", "publisher_name", "publisher_id"]]
    df_claims["claim_char"] = df_claims["claim_id"].str[0]
    df_claims["publisher_char"] = df_claims["publisher_id"].str.slice(0, 2)
    df_claims["cannonical_url"] = (
        +df_claims["publisher_name"].astype(str)
        + "#"
        + df_claims["publisher_char"].astype(str)
        + "/"
        + df_claims["name"].astype(str)
        + "#"
        + df_claims["claim_char"].astype(str)
    )
    df_claims["cannonical_url"] = df_claims["cannonical_url"].astype(str)
    return df_claims["cannonical_url"]


# Load unavailable data of streams from sdk
def sync_stream_data(df):
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

    # Sdk api request
    urls = df_results["cannonical_url"].tolist()
    payload = {"urls": urls}
    res = lbry_proxy("resolve", payload)
    if res:
        if "error" in res:
            print(res["error"])
            return pd.DataFrame()
        if "result" in res:
            res = res["result"]
    else:
        return pd.DataFrame()

    for url in urls:
        # Claim metadata
        metadata = res[url]
        # default values
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
            claim_status = "active"

        # Get claim stats
        if "meta" in metadata:
            if "reposted" in metadata["meta"]:
                claim_reposted = metadata["meta"]["reposted"]
            if "trending_mixed" in metadata["meta"]:
                claim_trending = metadata["meta"]["trending_mixed"]

        # Get claim value metadata
        if "value" in metadata:
            if "license" in metadata["value"]:
                claim_license = metadata["value"]["license"]
            if "languages" in metadata["value"]:
                claim_languages = metadata["value"]["languages"]
            if "tags" in metadata["value"]:
                claim_tags = set(metadata["value"]["tags"])

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

    # Append columns
    df_results["tags"] = tags
    df_results["status"] = status
    df_results["license"] = licenses
    df_results["reposted"] = reposted
    df_results["trending"] = trending
    df_results["languages"] = languages
    df_results["perm_url"] = perm_urls

    # Return new dataframe
    return df_results
