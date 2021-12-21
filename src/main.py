# Command line interface
import click
import asyncio
from elastic import Elastic
from sync import sync_elastic_search, sync_collections_metadata
from utils import remove_cache
from analysis import scan
from stats import fetch_stats
from status import main_status
from vocabulary import update_music_genres
from feed import update_root_feed

# Main command
@click.group()
def cli():
    pass


@cli.command()
def stats():
    fetch_stats()


# Initial sync
@cli.command()
def sync():
    scan()


# Sync cache data
@cli.command()
def sync_cache():
    sync_elastic_search()


# Retry failed sync
@cli.command()
def retry_sync():
    scan(main_status.status["chunk_index"])


# Clear cache files
@cli.command()
def clear_cache():
    remove_cache()


# Drop all data
@cli.command()
def drop():
    remove_cache()
    Elastic().destroy_data()


# Drop all data
@cli.command()
def update():
    update_music_genres()


# Sync collections on lbry channel
@cli.command()
def collections():
    sync_collections_metadata()


# Sync collections on lbry channel
@cli.command()
def feed():
    update_root_feed()
