import os
import json

MULTILINGUAL = {}
MULTILINGUAL_PATH = os.path.join("data", "multilingual.json")


with open(MULTILINGUAL_PATH, "r") as f:
    MULTILINGUAL = json.load(f)

GENRES = {}
GENRES_PATH = os.path.join("data", "genres.json")

# Read file
with open(GENRES_PATH, "r") as f:
    GENRES = json.load(f)
