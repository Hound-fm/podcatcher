import pandas as pd
from elastic import Elastic
from .events import process_publish_events, process_repost_events

# All feed is generated from stream index
def update_root_feed():
    el = Elastic()
    df_stream = el.get_df("stream", None)
    df_stream = df_stream.reset_index().rename(columns={"index": "stream_id"})
    df_stream = df_stream.sort_values(by="release_date", ascending=False)
    df_repost_events = process_repost_events(df_stream)
    df_publish_events = process_publish_events(df_stream)
    df_events = pd.concat([df_publish_events, df_repost_events])
    # Sort by new events
    # df_events.event_date = df_events.event_date.dt.date
    el.destroy_index("event")
    df_events.event_date = df_events.event_date.dt.tz_localize(None)
    df_events = df_events.sort_values(by="event_date", ascending=False)
    print(df_events.event_date.dtype)
    el.append_df_chunk("event", df_events)
    # Todo get reposts, supports and tips
