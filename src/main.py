# Command line interface
import click

# Init cli
@click.group()
def cli():
    pass


# Commands:
@cli.command()
def sync():
    pass
