import pathlib

import click

from aced_submission import NaturalOrderGroup
from aced_submission.fhir_store import fhir_store
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
cli.add_command(fhir_store)


@cli.command(name='schema-publish')
@click.argument('dictionary_path', default='iceberg/schemas/gen3/aced.json')
@click.option('--bucket', default="s3://aced-public", help="Bucket target", show_default=True)
@click.option('--production', default=False, is_flag=True, show_default=True,
              help="Write to aced.json, otherwise aced-test.json")
def schema_publish(dictionary_path, bucket, production):
    """Copy dictionary to s3 (note:aws cli dependency)"""

    dictionary_path = pathlib.Path(dictionary_path)
    assert dictionary_path.is_file(), f"{dictionary_path} should be a path"
    click.echo(f"Writing schema into {bucket}")
    dictionary_path.is_file()
    import subprocess
    if production:
        cmd = f"aws s3 cp {dictionary_path} {bucket}/aced.json".split(' ')
    else:
        cmd = f"aws s3 cp {dictionary_path} {bucket}/aced-test.json".split(' ')
    s3_cp = subprocess.run(cmd)
    assert s3_cp.returncode == 0, s3_cp
    print("OK")


if __name__ == '__main__':
    cli()
