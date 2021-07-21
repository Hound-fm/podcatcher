# Command line interface
import click
import asyncio
from analysis import full_scan

# Main command
@click.group()
def cli():
    pass


# Initial sync
@cli.command()
def sync():
    full_scan()


# Retry failed sync
@cli.command()
def retry_sync():
    pass
    # full_scan()
