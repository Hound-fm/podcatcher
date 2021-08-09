# Command line interface
import click
import asyncio
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


# Retry failed sync
@cli.command()
def retry_sync():
    scan(main_status.status["chunk_index"])


# Clear cache files
@cli.command()
def clear_cache():
    remove_cache()
