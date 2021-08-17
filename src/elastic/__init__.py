import eland as ed
from logger import console
from config import config
from constants import STREAM_TYPES, CHANNEL_TYPES
from elasticsearch import Elasticsearch
from .definitions import INDEX, INDICES, MAPPINGS_STREAM, MAPPINGS_CHANNEL


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
            # Destroy all autocomplete indices
            self.destroy_cache_indices()
            self.destroy_autocomplete_indices()
        except Exception as error:
            console.error("ELASTIC_SEARCH", error)

    def destroy_cache_indices(self):
        # Destroy main indices
        for index in INDICES:
            if self.client.indices.exists(index=index):
                self.client.indices.delete(index=index)

    def destroy_autocomplete_indices(self):
        try:
            # Append autocomplete prefix
            for index in {*STREAM_TYPES, *CHANNEL_TYPES}:
                index = f"{index}_autocomplete"
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
    def append_df_chunk(self, index_name, df):
        mappings = None
        # Select mappings
        if index_name == INDEX["STREAM"]:
            mappings = MAPPINGS_STREAM

        if index_name == INDEX["CHANNEL"]:
            mappings = MAPPINGS_CHANNEL

        # Use pandas index for elasticsearch id
        df = df.set_index(f"{index_name}_id")

        ed.pandas_to_eland(
            df,
            es_client=self.client,
            es_refresh=True,
            es_if_exists="append",
            es_dest_index=index_name,
            es_type_overrides=mappings,
        )

    def generate_autocomple_index(self, index, source, template_mappings):
        # Append autocomplete prefix
        dest_index = f"{index}_autocomplete"

        reindex_body = {
            "dest": {"index": dest_index},
            "source": source,
        }

        autocomplete_body = {"mappings": {"properties": template_mappings}}

        # Create empty index if doesn't exists
        if not self.client.indices.exists(index=dest_index):
            self.client.indices.create(index=dest_index, body=autocomplete_body)

        # Reindex source data
        self.client.reindex(body=reindex_body)
