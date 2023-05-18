import click

from aced_submission import NaturalOrderGroup
from aced_submission.meta import meta
from aced_submission.uploader import files
import logging


FORMAT = '%(levelname)s:%(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)


@click.group(cls=NaturalOrderGroup)
def cli():
    pass


cli.add_command(meta)
cli.add_command(files)

if __name__ == '__main__':
    cli()
