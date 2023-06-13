import threading
import uuid

import click
from fhir.resources import FHIRAbstractModel  # noqa

from aced_submission import NaturalOrderGroup
from aced_submission.dir_to_study.transform import cli as dir_to_study
from aced_submission.meta_graph_load import meta_upload

from aced_submission.meta_flat_load import cli as meta_flat_load_cli
from aced_submission.meta_discovery_load import discovery_load

LINKS = threading.local()
CLASSES = threading.local()
IDENTIFIER_LIST_SIZE = 8

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


@click.group(cls=NaturalOrderGroup)
def meta():
    """Project data (ResearchStudy, ResearchSubjects, Patient, etc.)."""
    pass


meta.add_command(dir_to_study)


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
@click.option('--dictionary_path',
              default=None,  # 'output/gen3',
              show_default=True,
              help='Path to data dictionary (path or url).')
@click.option('--config_path',
              default='config.yaml',
              show_default=True,
              help='Path to config file.')
def _meta_upload(source_path, program, project, credentials_file, silent, dictionary_path, config_path):
    """Copy simplified json into Gen3."""
    meta_upload(source_path, program, project, credentials_file, silent, dictionary_path, config_path,
                file_name_pattern='**/*.ndjson')


meta.add_command(meta_flat_load_cli)


@meta.group(name='discovery')
def discovery():
    """Gen3 discovery database."""
    pass


@discovery.command('load')
@click.option('--program', default="aced", show_default=True,
              help='Gen3 "program"')
@click.option('--credentials_file', default='~/.gen3/credentials.json', show_default=True,
              help='API credentials file downloaded from gen3 profile.')
def discovery(program, credentials_file):
    """Writes project information to discovery metadata-service"""
    discovery_load(program, credentials_file)


if __name__ == '__main__':
    meta()
