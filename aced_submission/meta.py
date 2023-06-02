import pathlib
import threading
import uuid

import click
from fhir.resources import FHIRAbstractModel  # noqa

from aced_submission import NaturalOrderGroup
from aced_submission.dir_to_study.transform import cli as dir_to_study

LINKS = threading.local()
CLASSES = threading.local()
IDENTIFIER_LIST_SIZE = 8

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


@click.group(cls=NaturalOrderGroup)
def meta():
    """Project data (ResearchStudy, ResearchSubjects, Patient, etc.)."""
    pass


meta.add_command(dir_to_study)


@meta.command(name='publish')
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


if __name__ == '__main__':
    meta()
