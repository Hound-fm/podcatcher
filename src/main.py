# Command line interface
import click
import asyncio
from elastic import Elastic
from sync import sync_elastic_search
from utils import remove_cache
from analysis import scan
from status import main_status

# Main command
@click.group()
def cli():
    pass


# Initial sync
@cli.command()
def sync():
    scan()


# Sync cache data
@cli.command()
def sync_cache():
    Elastic()
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
