# Command line interface
import click
from chainquery import query, bulk_fetch_streams

# Init cli
@click.group()
def cli():
    pass


# Commands:
@cli.command()
def sync():
    pass
