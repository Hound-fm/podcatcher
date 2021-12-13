import os
import json
import httpx
from bs4 import BeautifulSoup
from logger import console

MULTILINGUAL = {}
MULTILINGUAL_PATH = os.path.join("data", "multilingual.json")


with open(MULTILINGUAL_PATH, "r") as f:
    MULTILINGUAL = json.load(f)

GENRES = {}
GENRES_PATH = os.path.join("data", "genres.json")

# Read file
with open(GENRES_PATH, "r") as f:
    GENRES = json.load(f)


def format_music_genre_name(n):
    return n.text.replace(" music", "").lower()


def update_music_genres():
    url = "https://musicbrainz.org/genres"
    try:
        res = httpx.get(url, timeout=20.0)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        content = soup.find(id="content").find("ul").find_all("a")
        genres = list(map(format_music_genre_name, content))

        if genres and len(genres) > 0:
            GENRES["MUSIC"] = genres
            with open(GENRES_PATH, "wt") as f:
                json.dump(GENRES, f, sort_keys=True, ensure_ascii=True, indent=2)

    # Handle http request errors
    except httpx.HTTPStatusError as exc:
        console.error(
            "MUSICBRAINZ",
            f"Error response {exc.response.status_code} while requesting {url}.",
        )

    # Handle request errors
    except httpx.RequestError as exc:
        console.error(
            "MUSICBRAINZ",
            f"An error occurred while requesting {url}.",
            action=exec,
        )
