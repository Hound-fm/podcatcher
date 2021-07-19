# Command line interface
import click
from analysis import full_scan
import asyncio

# Init cli
@click.group()
def cli():
    pass


# Commands:
@cli.command()
def sync():
    full_scan()
