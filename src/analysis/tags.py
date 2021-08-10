import numpy as np
import pandas as pd
from utils import unique_clean_list
from constants import (
    CHANNEL_TYPE,
    CHANNEL_TYPES,
    STREAM_TYPE,
    STREAM_TYPES,
    FILTER_TAGS,
)
from vocabulary import GENRES, MULTILINGUAL

CLAIM_TYPE = "stream"

# Select dominant stream type
def select_dominant_category(categories):
    CATEGORIES_LIST = STREAM_TYPES if (CLAIM_TYPE == "stream") else CHANNEL_TYPES
    c = pd.Categorical(categories, ordered=True, categories=CATEGORIES_LIST)
    return c.max()


# Select known genres
def select_dominant_genres(df_tags, claim_type="stream"):
    COLUMN_TYPE = f"{claim_type}_type"
    CATEGORIES = STREAM_TYPE if (claim_type == "stream") else CHANNEL_TYPE

    conditions = [
        # This tag defines the audiobook genre
        df_tags[COLUMN_TYPE] == CATEGORIES["AUDIOBOOK"],
        # This tag defines the podcast genre
        df_tags[COLUMN_TYPE] == CATEGORIES["PODCAST"],
        # This tag defines the music genre
        df_tags[COLUMN_TYPE] == CATEGORIES["MUSIC"],
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


def process_tags(df, claim_type="stream"):
    CLAIM_ID = "stream_id" if (claim_type == "stream") else "channel_id"
    df_tags = df[["tags", CLAIM_ID]].explode("tags")
    df_tags = df_tags.rename(columns={"tags": "tag_name"})
    # Format tag name
    df_tags["tag_name"] = df_tags["tag_name"].astype(str)
    df_tags["tag_name"] = df_tags["tag_name"].str.lower()
    df_tags["tag_name"] = df_tags["tag_name"].str.strip()
    df_tags["tag_name"] = df_tags["tag_name"].str.replace("-and-", " and ")
    # Filter tags
    df_tags = df_tags.loc[~df_tags["tag_name"].isin(FILTER_TAGS)]
    return process_special_tags(df_tags, claim_type)


def process_special_tags(df, claim_type="stream"):
    global CLAIM_TYPE
    CLAIM_TYPE = claim_type
    COLUMN_TYPE = f"{CLAIM_TYPE}_type"
    CLAIM_ID = "stream_id" if (CLAIM_TYPE == "stream") else "channel_id"
    CATEGORIES = STREAM_TYPE if (CLAIM_TYPE == "stream") else CHANNEL_TYPE
    CATEGORIES_LIST = STREAM_TYPES if (CLAIM_TYPE == "stream") else CHANNEL_TYPES

    df_tags = df.copy()
    # Find multilingual or alias version and correct tag_names:
    df_tags.loc[
        df_tags["tag_name"].str.contains("|".join(MULTILINGUAL["MUSIC"]), case=False),
        "tag_name",
    ] = CATEGORIES["MUSIC"]

    if CLAIM_TYPE == "channel":
        df_tags.loc[
            df_tags["tag_name"].str.contains(
                "|".join(MULTILINGUAL["ARTIST"]), case=False
            ),
            "tag_name",
        ] = CATEGORIES["MUSIC"]

    df_tags.loc[
        df_tags["tag_name"].str.contains("|".join(MULTILINGUAL["PODCAST"]), case=False),
        "tag_name",
    ] = CATEGORIES["PODCAST"]

    df_tags.loc[
        df_tags["tag_name"].str.contains(
            "|".join(MULTILINGUAL["AUDIOBOOK"]), case=False
        ),
        "tag_name",
    ] = CATEGORIES["AUDIOBOOK"]

    conditions = [
        # This tag defines the category type
        df_tags["tag_name"].isin(CATEGORIES_LIST),
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
        COLUMN_TYPE,
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
        df_tags.groupby(CLAIM_ID)
        .agg(
            # Tags
            tags=("tags", unique_clean_list),
            # Genres
            music_genre=("music_genre", unique_clean_list),
            podcast_genre=("podcast_genre", unique_clean_list),
            audiobook_genre=("audiobook_genre", unique_clean_list),
            # Type
            **{COLUMN_TYPE: (COLUMN_TYPE, select_dominant_category)},
        )
        .reset_index()
    )

    df_tags[COLUMN_TYPE] = df_tags[COLUMN_TYPE]
    df_tags["genres"] = select_dominant_genres(df_tags, CLAIM_TYPE)

    # Return relevant columns
    return df_tags[[CLAIM_ID, COLUMN_TYPE, "genres", "tags"]]
