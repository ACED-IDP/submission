import pathlib
import threading
import uuid

import click
from fhir.resources import FHIRAbstractModel  # noqa

from aced_submission import NaturalOrderGroup
from aced_submission.dir_to_study.transform import cli as dir_to_study
from aced_submission.meta_uploader import meta_upload

LINKS = threading.local()
CLASSES = threading.local()
IDENTIFIER_LIST_SIZE = 8

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


@click.group(cls=NaturalOrderGroup)
def meta():
    """Project data (ResearchStudy, ResearchSubjects, Patient, etc.)."""
    pass


meta.add_command(dir_to_study)


@meta.command(name='schema-publish')
@click.argument('dictionary_path', default='iceberg/schemas/gen3/aced.json')
@click.option('--bucket', default="s3://aced-public", help="Bucket target", show_default=True)
@click.option('--production', default=False, is_flag=True, show_default=True,
              help="Write to aced.json, otherwise aced-test.json")
def schema_publish(dictionary_path, bucket, production):
    """Copy dictionary to s3 (note:aws cli dependency)"""

    dictionary_path = pathlib.Path(dictionary_path)
    assert dictionary_path.is_file(), f"{dictionary_path} should be a path"
    click.echo(f"Writing schema into {bucket}")
    import subprocess
    if production:
        cmd = f"aws s3 cp {dictionary_path} {bucket}".split(' ')
    else:
        cmd = f"aws s3 cp {dictionary_path} {bucket}/aced-test.json".split(' ')
    s3_cp = subprocess.run(cmd)
    assert s3_cp.returncode == 0, s3_cp
    print("OK")


@meta.group(name='graph')
def graph():
    """Gen3 Graph data (nodes, edges, etc.)."""
    pass


@graph.command(name='upload')
@click.option('--source_path', required=False, default=None, show_default=True,
              help='Path on local file system')
@click.option('--program', required=True, show_default=True,
              help='Gen3 program')
@click.option('--project', required=True, show_default=True,
              help='Gen3 project')
@click.option('--credentials_file', default='~/.gen3/credentials.json', show_default=True,
              help='API credentials file downloaded from gen3 profile.')
@click.option('--silent', default=False, is_flag=True, show_default=True,
              help="No progress bar, or other output")
@click.option('--config_path',
              default='config.yaml',
              show_default=True,
              help='Path to config file.')
def upload_document_reference(source_path, program, project, credentials_file, silent, config_path):
    """Copy simplified json into Gen3."""
    meta_upload(source_path, program, project, credentials_file, silent, config_path)


if __name__ == '__main__':
    meta()
