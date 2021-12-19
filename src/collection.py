import os
import json

COLLECTIONS = {}
COLLECTIONS_PATH = os.path.join("data", "collections.json")

# Read file
with open(COLLECTIONS_PATH, "r") as f:
    COLLECTIONS = json.load(f)
