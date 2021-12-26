import pandas as pd
from utils import drop_consecutive
from sync import sync_authors
from chainquery import query, queries


def fetch_reposts_df(stream_ids):
    results = query(queries.bulk_fetch_reposts(list(stream_ids)))
    if results and len(results) > 0:
        df_reposts = pd.DataFrame.from_records(results)
        df_authors = sync_authors(df_reposts["author_id"].unique())
        df_reposts = df_reposts.join(df_authors.set_index("author_id"), on="author_id")
        df_reposts = df_reposts.loc[df_reposts["author_name"].notnull()]
        df_reposts["event_type"] = "repost"
        return df_reposts


def process_repost_events(df_stream):
    df_stream_data = df_stream.copy()
    # Get valid reposts data
    df_reposts_data = fetch_reposts_df(df_stream_data["stream_id"])
    # Link channel data to event:
    df_events = df_stream_data.rename(columns={"stream_id": "event_stream_id"})
    df_events["event_stream_id"] = df_events["event_stream_id"].astype(str)
    df_reposts_data["event_stream_id"] = df_reposts_data["event_stream_id"].astype(str)
    # Merge reposts adata: author, stream_id, etc...
    df_events = df_events.merge(df_reposts_data, on="event_stream_id")
    df_events = df_events.loc[df_events["event_type"].notnull()]
    # Generate event identifier: {event_type}:{claim_id}:{timestamp}
    df_events["event_id"] = (
        df_events["event_type"]
        + ":"
        + df_events["event_stream_id"]
        + ":"
        + df_events["event_date"].astype(str)
    )
    # Prevent self-repost / self-discover
    df_events = df_events.loc[df_events["channel_name"] != df_events["author_name"]]
    # Sort by first reposts
    df_events = df_events.sort_values(by=["event_stream_id", "event_date"])
    # Assign discover event: "first repost"
    # Todo: combine with first support / tips
    df_events.loc[
        (df_events[["event_stream_id"]].shift() != df_events[["event_stream_id"]]).any(
            axis=1
        ),
        "event_type",
    ] = "discover"
    df_events["event_id"] = df_events["event_id"].replace("repost", "discover")
    df_events.event_date = pd.to_datetime(
        df_events.event_date, infer_datetime_format=True, unit="ns", utc=True
    )
    return df_events


# Generate publish events
def process_publish_events(df_stream):
    df_events = df_stream.copy()
    # Set event type
    df_events["event_type"] = "publish"
    # Transform to events:
    # Link channel data to event:
    df_events["author_id"] = df_events["channel_id"]
    df_events["author_url"] = df_events["channel_url"]
    df_events["author_name"] = df_events["channel_name"]
    df_events["author_title"] = df_events["channel_title"]
    df_events["event_date"] = df_events["release_date"]
    # Rename columns to event columns
    df_events = df_events.rename(columns={"stream_id": "event_stream_id"})
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
    df_events.event_date = pd.to_datetime(
        df_events.event_date, infer_datetime_format=True, unit="s", utc=True
    )
    return df_events
