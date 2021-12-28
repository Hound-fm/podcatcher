import spacy
from ftlangdetect import detect
from vocabulary import GENRES, MULTILINGUAL

nlp_en = spacy.load("en_core_web_sm")


def get_artist_score(text):
    # result = detect(text=text, low_memory=True)
    # if result and 'lang' in result:
    # print(result['lang'])
    doc = nlp_en(text)

    genres = set()
    score = 0.0
    score_boost = 1.0

    for token in doc:
        if not token.is_stop and token.pos_ == "NOUN":
            tag = token.lemma_
            if tag in MULTILINGUAL["MUSIC"]:
                score_boost += 0.15
            if tag in MULTILINGUAL["ARTIST"]:
                score += 0.75
            if tag in GENRES["MUSIC"]:
                genres.add(tag)
                score_boost += 0.15
    # Final score
    score = score * score_boost
    return score
