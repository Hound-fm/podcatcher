from elastic import Elastic
from .events import process_publish_events, process_repost_events

# All feed is generated from stream index
def update_root_feed():
    el = Elastic()
    df_stream = el.get_df("stream", None)
    df_stream = df_stream.reset_index().rename(columns={"index": "stream_id"})
    df_stream = df_stream.sort_values(by="release_date", ascending=False)
    process_repost_events(df_stream)
    # df_events = process_publish_events(df_stream)

    # Todo get reposts, supports and tips
