import numpy as np
import pandas as pd
from utils import unique_clean_list
from constants import STREAM_TYPE, STREAM_TYPES, FILTER_TAGS
from vocabulary import GENRES, MULTILINGUAL

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
