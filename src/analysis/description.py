import spacy
import pandas as pd
from ftlangdetect import detect
from vocabulary import GENRES, MULTILINGUAL

nlp_de = spacy.load("de_core_news_sm")
nlp_en = spacy.load("en_core_web_sm")
nlp_es = spacy.load("es_core_news_sm")
nlp_fr = spacy.load("fr_core_news_sm")


def clamp(minvalue, value, maxvalue):
    return max(minvalue, min(value, maxvalue))


def nlp_multilingual(text):
    result = detect(text=text, low_memory=True)
    if "lang" in result:
        lang = result["lang"]
        # French
        if lang == "fr":
            return nlp_fr(text)
        # German
        elif lang == "de":
            return nlp_de(text)
        # Spanish
        elif lang == "es":
            return nlp_es(text)
    # Default to english
    return nlp_en(text)


def get_stream_metadata_score(row):
    series_row = row.copy()
    text = str(series_row["description"])
    genres = set(series_row["genres"])
    score_boost = 0

    # Validate description text
    if not text:
        return series_row

    # spaCy: Part-of-speech tagging
    doc = nlp_multilingual(text)

    # Score boost by short description
    if len(text) < 200:
        score_boost += 0.5

    return series_row


def get_channel_metadata_score(row):
    series_row = row.copy()
    text = str(series_row["description"])
    genres = set(series_row["genres"])

    # Validate description text
    if not text:
        return series_row

    # spaCy: Part-of-speech tagging
    doc = nlp_multilingual(text)

    # Default values
    music_genres = set()
    # Score values
    score = 0.0
    score_boost = 0.75
    score_genre_boost = 0.0
    score_category_boost = 0.0

    # Score boost by genres / tags
    if len(genres):
        for genre in genres:
            if genre in GENRES["MUSIC"]:
                music_genres.add(genre)
                score_genre_boost = clamp(0, score_genre_boost + 0.35, 1.5)
            if genre in GENRES["PODCAST"]:
                score -= 0.025

    # Score boost by description keywords:
    for token in doc:
        if not token.is_stop and token.pos_ == "NOUN":
            tag = token.lemma_
            tag_text = token.text

            # Channel is probably a podcast
            if tag in MULTILINGUAL["PODCAST"]:
                score -= 0.25

            # Channel is related to music
            if tag in MULTILINGUAL["MUSIC"]:
                if score_category_boost == 0:
                    score_category_boost = 0.35
                else:
                    score_category_boost = clamp(0, score_category_boost + 0.15, 1.25)

            # Channel description mentions or belongs to an artist
            if tag in MULTILINGUAL["ARTIST"]:
                if score <= 0:
                    if tag_text.lower() == tag:
                        score_boost += 0.25
                        score += 0.5
                    else:
                        score += 0.15

                score_boost = clamp(0, score_boost + 0.15, 1.35)
            # Channel description contains music genres
            if tag in GENRES["MUSIC"]:
                music_genres.add(tag)
                score_genre_boost = clamp(0, score_genre_boost + 0.35, 1.5)

    # Channel is related to music
    is_relevant = (score_category_boost > 0) or (score_genre_boost > 0)

    # Score bost by name mention
    if score > 0:
        channel_title = row["channel_title"]
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                if channel_title.lower().startswith(ent.text.lower()):
                    if is_relevant:
                        score_boost += 0.95
                    else:
                        socre_boost += 0.65

    # Score boost by short description
    if len(text) < 200:
        if is_relevant:
            score_boost += 0.5
        else:
            score_boost += 0.1

    # Apply score boost
    score = score * (score_boost + score_genre_boost + score_category_boost)

    if score > 0.85:
        series_row["genres"] = list(music_genres)
        series_row["channel_type"] = "artist"

    # Update score
    series_row["channel_metadata_score"] = score

    # if score >= 0.6:
    #   print(series_row[["channel_id", "channel_title", "channel_metadata_score"]])

    return series_row


def process_channel_description(df):
    # Copy relevant columns
    df_nlp = df[
        [
            "channel_id",
            "channel_type",
            "channel_metadata_score",
            "channel_title",
            "description",
            "genres",
        ]
    ].copy()

    # Filters
    # Filter empty or invalid descriptions
    # Longer descriptions provide more info:
    MIN_DESCRIPTION_LENGTH = 15
    df_nlp = df_nlp.loc[df_nlp.description.notnull()]
    df_nlp.description = df_nlp.description.astype(str).str.strip()
    df_nlp = df_nlp.loc[
        (df_nlp.description != "")
        & (df_nlp.description.str.len() > MIN_DESCRIPTION_LENGTH)
    ]

    if not df_nlp.empty:
        df_nlp["channel_title"] = df_nlp["channel_title"].astype(str)
        df_nlp = df_nlp.loc[~df_nlp["channel_title"].str.contains(" - ", na=False)]

    if df_nlp.empty:
        return df_nlp

    # Format description
    df_nlp["description"] = df_nlp["description"].str.replace(r"\s\s+", " ", regex=True)
    df_nlp["description"] = df_nlp["description"].str.replace(r"[\r\n]", "", regex=True)
    df_nlp["description"] = df_nlp["description"].str.strip()

    # Run analysis for each row
    df_nlp = df_nlp.apply(get_channel_metadata_score, axis=1)

    return df_nlp[["channel_id", "channel_type", "channel_metadata_score", "genres"]]


def process_stream_description(df):
    # Copy relevant columns
    df_nlp = df[
        [
            "title",
            "stream_id",
            "channel_type",
            "channel_title",
            "description",
            "genres",
        ]
    ].copy()

    # Filters
    # Filter empty or invalid descriptions
    # Longer descriptions provide more info:
    MIN_DESCRIPTION_LENGTH = 15
    df_nlp = df_nlp.loc[df_nlp.description.notnull()]
    df_nlp.description = df_nlp.description.astype(str).str.strip()
    df_nlp = df_nlp.loc[
        (df_nlp.description != "")
        & (df_nlp.description.str.len() > MIN_DESCRIPTION_LENGTH)
    ]

    if df_nlp.empty:
        return df_nlp

    # Format description
    df_nlp["description"] = df_nlp["description"].str.replace(r"\s\s+", " ", regex=True)
    df_nlp["description"] = df_nlp["description"].str.replace(r"[\r\n]", "", regex=True)
    df_nlp["description"] = df_nlp["description"].str.strip()

    # Initialize metadata score
    df_nlp["stream_metadata_score"] = 0

    # Run analysis for each row
    df_nlp = df_nlp.apply(get_stream_metadata_score, axis=1)

    return df_nlp[["stream_id", "stream_metadata_score", "genres"]]
