import eland as ed
from logger import console
from config import config
from elasticsearch import Elasticsearch
from constants import ELASTIC_INDICES

MAPPINGS_STREAM = {
    # keywords
    "title": "text",
    "name": "text",
    "tags": "keyword",
    "genres": "keyword",
    "trending": "float",
    "thumbnail": "text",
    "reposted": "integer",
    "license": "text",
    "channel_id": "text",
    "channel_name": "text",
    "channel_title": "text",
    "stream_type": "keyword",
}

MAPPINGS_CHANNEL = {
    # keywords
    "trending": "float",
    "thumbnail": "text",
    "channel_name": "text",
    "channel_title": "text",
    "channel_type": "keyword",
}


class Elastic:
    def __init__(self):
        if "ELASTIC_HOST" in config:
            host = config["ELASTIC_HOST"]
            # No authentication provided
            if not config.keys() & {"ELASTIC_USER", "ELASTIC_PASSWORD"}:
                self.client = Elasticsearch([host], http_compress=True)
            else:
                # HTTP authentication
                self.client = Elasticsearch(
                    [host],
                    http_auth=(config["ELASTIC_USER"], config["ELASTIC_PASSWORD"]),
                    http_compress=True,
                )

    # Generate data structure:
    def build_data_schema(self):
        try:
            for index in self.index_list:
                if not self.client.indices.exists(index=index):
                    self.client.indices.create(index=index)
        except Exception as error:
            console.error("ELASTIC_SEARCH", error)

    def destroy_data(self):
        try:
            for index in ELASTIC_INDICES:
                if self.client.indices.exists(index=index):
                    self.client.indices.delete(index=index)
        except Exception as error:
            console.error("ELASTIC_SEARCH", error)

    # Get data frame from index
    def get_df(self, index, columns=[]):
        proxy = None
        if columns:
            proxy = ed.DataFrame(self.client, es_index_pattern=index, columns=columns)
        else:
            proxy = ed.DataFrame(self.client, es_index_pattern=index)
        pandas_df = ed.eland_to_pandas(proxy)
        return pandas_df

    # Append chunk from dataFrame to elastic-search
    def append_df_chunk(self, index_name, df, chunk_type="stream"):
        mappings = None
        # Select mappings
        if chunk_type == "stream":
            mappings = MAPPINGS_STREAM

        if chunk_type == "channel":
            mappings = MAPPINGS_CHANNEL

        # Use pandas index for elasticsearch id
        df = df.set_index(f"{chunk_type}_id")

        ed.pandas_to_eland(
            df,
            es_client=self.client,
            es_refresh=True,
            es_if_exists="append",
            es_dest_index=index_name,
            es_type_overrides=mappings,
        )
