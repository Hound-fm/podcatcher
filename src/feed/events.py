import pandas as pd
from utils import drop_consecutive
from sync import sync_author_metadata
from chainquery import query, queries


def fetch_reposts_df(stream_ids):
    results = query(queries.bulk_fetch_reposts(list(stream_ids)))
    if results and len(results) > 0:
        df_reposts = pd.DataFrame.from_records(results)
        df_authors = sync_author_metadata(df_reposts["author_id"].unique())
        print(df_authors)


def process_repost_events(df_stream):
    df_stream_data = df_stream.copy()
    df_reposts = fetch_reposts_df(df_stream_data["stream_id"])


# Generate publish events
def process_publish_events(df_stream):
    df_events = df_stream.copy()[
        [
            "title",
            "genres",
            "duration",
            "thumbnail",
            "stream_id",
            "stream_type",
            "channel_id",
            "channel_url",
            "channel_name",
            "channel_title",
            "release_date",
        ]
    ]
    # Set event type
    df_events["event_type"] = "publish"
    # Transform to events:
    # Link channel data to event:
    df_events["event_channel_id"] = df_events["channel_id"]
    df_events["event_channel_url"] = df_events["channel_url"]
    df_events["event_channel_name"] = df_events["channel_name"]
    df_events["event_channel_title"] = df_events["channel_title"]
    # Rename columns to event columns
    df_events = df_events.rename(
        columns={
            # Event data
            "release_date": "event_date",
            # Map publisher data (author data):
            # Publisher is also the author
            "channel_id": "author_id",
            "channel_url": "author_url",
            "channel_name": "author_name",
            "channel_title": "author_title",
            # Map stream nested data
            "url": "event_stream_url",
            "title": "event_stream_title",
            "genres": "event_stream_genres",
            "duration": "event_stream_duration",
            "thumbnail": "event_stream_thumbnail",
            "stream_id": "event_stream_id",
            "stream_type": "event_stream_type",
        }
    )
    # Generate event identifier: {event_type}:{claim_id}:{timestamp}
    df_events["event_id"] = (
        df_events["event_type"]
        + ":"
        + df_events["event_stream_id"]
        + ":"
        + df_events["event_date"].astype(str)
    )
    # Keep feed clean and simple
    df_events = drop_consecutive(df_events, ["author_id"])
    return df_events
