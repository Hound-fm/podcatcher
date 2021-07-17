from typing import Final

# Constants values
TAG_ID: Final = {
    "MATURE": [1],
    "PODCAST": [1847, 1717, 2900186],
}

CLAIM_TYPE: Final = {
    "STREAM": "stream",
    "CHANNEL": "channel",
    "CLAIM_LIST": "claimList",
    "CLAIM_REPOST": "claimreference",
}

CONTENT_TYPE_AUDIO: Final = "audio"

# Public chainquery API:
CHAINQUERY_API: Final = "https://chainquery.lbry.io/api/sql"
