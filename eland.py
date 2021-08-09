import eland as ed
from logger import log
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Elastic:
    def __init__(self, host):
        self.index_list = [
            "artists",
            "music_recordings",
            "podcast_series",
            "podcast_episodes",
        ]
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
            for index in self.index_list:
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
    def append_df_chunk(self, pd_df, index_name, overrides):
        ed.pandas_to_eland(
            pd_df,
            es_client=self.client,
            es_refresh=True,
            es_if_exists="append",
            es_dest_index=index_name,
            es_type_overrides=overrides,
        )
