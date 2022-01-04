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

    # Reset genres
    series_row["genres"] = None

    # Default values
    music_genres = set()
    podcast_genres = set()
    # Artist score values
    music_score = 0.0
    music_score_boost = 0.75
    music_score_genre_boost = 0.0
    music_score_category_boost = 0.0
    # Podcast score values
    podcast_score = 0.0
    podcast_score_boost = 0.75
    podcast_score_genre_boost = 0.0
    podcast_score_category_boost = 0.0

    # Validate description text
    if not text:
        return series_row

    # Stream belongs to an artist ( probably a song )
    if series_row["channel_type"] == "artist":
        music_score += 0.35
        music_score_genre_boost = 1.0

    # Stream belongs to a podcast ( probably an episode )
    if series_row["channel_type"] == "podcast_series":
        music_score -= 0.25
        podcast_score += 0.35
        podcast_score_genre_boost = 1.0

    # Score boost by genres / tags
    if len(genres):
        for genre in genres:
            if genre in GENRES["MUSIC"]:
                music_genres.add(genre)
                music_score_genre_boost = clamp(0, music_score_genre_boost + 0.45, 2.25)
            if genre in GENRES["PODCAST"]:
                music_score -= 0.025
                podcast_genres.add(genre)
                podcast_score_genre_boost = clamp(
                    0, podcast_score_genre_boost + 0.45, 2.25
                )

    # spaCy: Part-of-speech tagging
    doc = nlp_multilingual(text)

    # Score boost by description keywords:
    for token in doc:
        if not token.is_stop and token.pos_ == "NOUN":
            tag = token.lemma_
            tag_text = token.text

            # Stream is related to music
            if tag in MULTILINGUAL["MUSIC"]:
                if music_score_category_boost == 0:
                    music_score_category_boost = 0.35
                else:
                    music_score_category_boost = clamp(
                        0, music_score_category_boost + 0.15, 2.0
                    )

            # Channel is related to podcasts
            if tag in MULTILINGUAL["PODCAST"] or tag in MULTILINGUAL["EPISODE"]:
                music_score -= 0.25
                if podcast_score <= 0:
                    if tag_text.lower() == tag:
                        podcast_score_boost += 0.35
                        podcast_score += 0.5
                    else:
                        podcast_score += 0.35
                if podcast_score_category_boost == 0:
                    podcast_score_category_boost = 0.5
                else:
                    podcast_category_boost = clamp(
                        0, podcast_score_category_boost + 0.25, 2.0
                    )

            # Channel description contains music genres
            if tag in GENRES["MUSIC"]:
                music_genres.add(tag)
                music_score_genre_boost = clamp(0, music_score_genre_boost + 0.35, 2.25)

            # Channel description contains podcast genres
            if tag in GENRES["PODCAST"]:
                podcast_genres.add(tag)
                podcast_score_genre_boost = clamp(
                    0, podcast_score_genre_boost + 0.35, 2.25
                )

    # Stream is related to music ( probably a song )
    is_music_relevant = (music_score_category_boost > 0) or (
        music_score_genre_boost > 0
    )

    # Stream is related to podcast ( probably an episode )
    is_podcast_relevant = (podcast_score_category_boost > 0) or (
        podcast_score_genre_boost > 0
    )

    # Score boost by short description
    if len(text) < 200:
        if is_music_relevant:
            music_score_boost += 0.5
        elif is_podcast_relevant:
            podcast_score_boost += 0.5

    # Apply score boost
    music_score = music_score * (
        music_score_boost + music_score_genre_boost + music_score_category_boost
    )

    # Apply score boost
    podcast_score = podcast_score * (
        podcast_score_boost + podcast_score_genre_boost + podcast_score_category_boost
    )

    if podcast_score >= 0.85:
        series_row["genres"] = list(podcast_genres)
        series_row["stream_type"] = "podcast_episode"
        series_row["stream_metadata_score"] = podcast_score

    elif music_score >= 0.85:
        series_row["genres"] = list(music_genres)
        series_row["stream_type"] = "music_recording"
        series_row["stream_metadata_score"] = music_score

    else:
        if music_score > podcast_score:
            series_row["stream_metadata_score"] = music_score
        elif podcast_score >= music_score:
            series_row["stream_metadata_score"] = podcast_score

    if series_row["stream_metadata_score"] > 3:
        print(
            # series_row["stream_id"],
            series_row["channel_title"],
            series_row["title"],
            series_row["channel_type"],
            series_row["stream_type"],
            series_row["stream_metadata_score"],
        )

    return series_row


def get_channel_metadata_score(row):
    series_row = row.copy()
    text = str(series_row["description"])
    genres = set(series_row["genres"])

    # Reset genres
    series_row["genres"] = None

    # Validate description text
    if not text:
        return series_row

    # spaCy: Part-of-speech tagging
    doc = nlp_multilingual(text)

    # Default values
    music_genres = set()
    podcast_genres = set()
    # Artist score values
    artist_score = 0.0
    artist_score_boost = 0.75
    artist_score_genre_boost = 0.0
    artist_score_category_boost = 0.0
    # Podcast score values
    podcast_score = 0.0
    podcast_score_boost = 0.75
    podcast_score_genre_boost = 0.0
    podcast_score_category_boost = 0.0

    # Score boost by genres / tags
    if len(genres):
        for genre in genres:
            if genre in GENRES["MUSIC"]:
                music_genres.add(genre)
                artist_score_genre_boost = clamp(
                    0, artist_score_genre_boost + 0.35, 1.5
                )
            if genre in GENRES["PODCAST"]:
                podcast_genres.add(genre)
                podcast_score_genre_boost = clamp(
                    0, podcast_score_genre_boost + 0.35, 1.5
                )

    # Score bost by name mention
    if artist_score > 0:
        channel_title = row["channel_title"]
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                if channel_title.lower().startswith(ent.text.lower()):
                    if is_music_relevant:
                        music_score_boost += 0.95
                    elif is_podcast_relevant:
                        podcast_score_boost += 0.95
                    else:
                        music_score_boost += 0.65
                        podcast_score_boost += 0.65

    # Score boost by description keywords:
    for token in doc:
        if token.pos_ == "PROPN":
            tag_text = token.lemma_.lower()
            if tag_text in MULTILINGUAL["PODCAST"]:
                artist_score = 0
                artist_score_boost = -0.5
                artist_score_category_boost = -0.5

                if podcast_score <= 0.5:
                    podcast_score += 0.25
                    podcast_score_boost += 0.25
                    podcast_score_category_boost += 0.15

        if not token.is_stop and token.pos_ == "NOUN":
            tag = token.lemma_
            tag_text = token.text

            # Channel is related to podcasts
            if tag in MULTILINGUAL["PODCAST"]:
                artist_score = 0
                artist_score_boost = -0.5
                artist_score_category_boost = -0.5

                if podcast_score <= 0:
                    if tag_text.lower() == tag:
                        podcast_score_boost += 0.25
                        podcast_score += 0.5
                    else:
                        podcast_score += 0.15
                if podcast_score_category_boost == 0:
                    podcast_score_category_boost = 0.35
                else:
                    podcast_category_boost = clamp(
                        0, podcast_score_category_boost + 0.15, 1.25
                    )

            # Channel is related to podcasts
            # Note: "episode" have a minor boost, is prefered to use the "podcast" keyword
            if tag in MULTILINGUAL["EPISODE"]:
                if podcast_score <= 0:
                    podcast_score_boost += 0.15
                    podcast_score += 0.35
                if podcast_score_category_boost == 0:
                    podcast_score_category_boost = 0.25
                else:
                    podcast_category_boost = clamp(
                        0, podcast_score_category_boost + 0.1, 1.25
                    )
            # Channel is related to music
            if (tag in MULTILINGUAL["MUSIC"]) and (podcast_score <= 0):
                if artist_score_category_boost == 0:
                    artist_score_category_boost = 0.35
                else:
                    artist_score_category_boost = clamp(
                        0, artist_score_category_boost + 0.15, 1.25
                    )

            # Channel description mentions or belongs to an artist
            if (tag in MULTILINGUAL["ARTIST"]) and (podcast_score <= 0):
                if artist_score <= 0:
                    if tag_text.lower() == tag:
                        artist_score_boost += 0.25
                        artist_score += 0.5
                    else:
                        artist_score += 0.15

                artist_score_boost = clamp(0, artist_score_boost + 0.15, 1.35)

            # Channel description contains music genres
            if tag in GENRES["MUSIC"]:
                music_genres.add(tag)
                artist_score_genre_boost = clamp(
                    0, artist_score_genre_boost + 0.35, 1.5
                )

            # Channel description contains podcast genres
            if tag in GENRES["PODCAST"]:
                podcast_genres.add(tag)
                podcast_score_genre_boost = clamp(
                    0, podcast_score_genre_boost + 0.35, 1.5
                )

    # Channel is related to music
    is_artist_relevant = (artist_score_category_boost > 0) or (
        artist_score_genre_boost > 0
    )
    is_podcast_relevant = (podcast_score_category_boost > 0) or (
        podcast_score_genre_boost > 0
    )

    # Score bost by name mention
    if is_podcast_relevant or is_artist_relevant:
        channel_title = row["channel_title"]
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                if channel_title.lower().startswith(ent.text.lower()):
                    if is_podcast_relevant:
                        podcast_score_boost += 0.95
                    elif is_artist_relevant:
                        artist_score_boost += 0.95

    # Score boost by short description
    if len(text) < 200:
        if is_podcast_relevant:
            podcast_score_boost += 0.5
        elif is_artist_relevant:
            artist_score_boost += 0.5

    # Apply score boost
    artist_score = artist_score * (
        artist_score_boost + artist_score_genre_boost + artist_score_category_boost
    )
    # Apply score boost
    podcast_score = podcast_score * (
        podcast_score_boost + podcast_score_genre_boost + podcast_score_category_boost
    )

    if podcast_score >= 0.85:
        series_row["genres"] = list(podcast_genres)
        series_row["channel_type"] = "podcast_series"
        series_row["channel_metadata_score"] = podcast_score

    elif artist_score >= 0.85:
        series_row["genres"] = list(music_genres)
        series_row["channel_type"] = "artist"
        series_row["channel_metadata_score"] = artist_score

    else:
        if podcast_score >= artist_score:
            series_row["channel_metadata_score"] = podcast_score
        elif artist_score > podcast_score:
            series_row["channel_metadata_score"] = artist_score

    if series_row["channel_metadata_score"] > 0:
        print(
            series_row["channel_id"],
            series_row["channel_title"],
            series_row["channel_type"],
            series_row["channel_metadata_score"],
        )

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
            "stream_type",
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

    # Run analysis for each row
    df_nlp = df_nlp.apply(get_stream_metadata_score, axis=1)

    return df_nlp[["stream_id", "stream_type", "stream_metadata_score", "genres"]]
