import eland as ed
from logger import log
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from constants import ELASTIC_INDICES

MAPPINGS_STREAM = {
    # keywords
    "title": "text",
    "name": "text",
    "trending": "float",
    "reposted": "integer",
    "channel_id": "text",
    "channel_name": "text",
    "channel_title": "text",
    "stream_type": "keyword",
}

MAPPINGS_CHANNEL = {
    # keywords
    "trending": "float",
    "reposted": "integer",
    "channel_name": "text",
    "channel_title": "text",
    "channel_type": "keyword",
}


class Elastic:
    def __init__(self, host="http://localhost:9200"):
        self.client = Elasticsearch([host], http_compress=True)

    # Generate data structure:
    def build_data_schema(self):
        try:
            for index in self.index_list:
                if not self.client.indices.exists(index=index):
                    self.client.indices.create(index=index)
        except Exception as error:
            log.error(error)

    def destroy_data(self):
        try:
            for index in ELASTIC_INDICES:
                if self.client.indices.exists(index=index):
                    self.client.indices.delete(index=index)
        except Exception as error:
            log.error(error)

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
    def append_df_chunk(self, index_name, pd_df, chunk_type="stream"):
        mappings = None
        # Select mappings
        if chunk_type == "stream":
            mappings = MAPPINGS_STREAM
        if chunk_type == "channel":
            mappings = MAPPINGS_CHANNEL

        # Use pandas index for elasticsearch id
        pd_df = pd_df.set_index(f"{chunk_type}_id")

        ed.pandas_to_eland(
            pd_df,
            es_client=self.client,
            es_refresh=True,
            es_if_exists="append",
            es_dest_index=index_name,
            es_type_overrides=mappings,
        )
