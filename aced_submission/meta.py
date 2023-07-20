import json
import pathlib
import sys
import threading
import uuid

import click
import yaml
from fhir.resources import FHIRAbstractModel  # noqa

from aced_submission import NaturalOrderGroup
from aced_submission.meta_graph_load import meta_upload, _table_mappings, _connect_to_postgres

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


@graph.command(name='mapping')
@click.option('--dictionary_path',
              default=None,  # 'output/gen3',
              show_default=True,
              help='Path to data dictionary (path or url).')
@click.option('--config_path',
              default='config.yaml',
              show_default=True,
              help='Path to config file.')
@click.option('--format', 'output_format',
              default='yaml',
              show_default=True,
              type=click.Choice(['yaml', 'json'], case_sensitive=False),
              )
def _meta_mapping(dictionary_path, config_path, output_format):
    """Show the table mappings for a data dictionary."""
    assert dictionary_path, '--dictionary_path is required'
    dictionary_dir = dictionary_path if 'http' not in dictionary_path else None
    dictionary_url = dictionary_path if 'http' in dictionary_path else None
    mappings = [mapping for mapping in _table_mappings(dictionary_dir, dictionary_url)]
    if output_format == 'yaml':
        yaml.dump(mappings, sys.stdout, default_flow_style=False)
    else:
        json.dump(mappings, sys.stdout, indent=2)


@graph.command(name='counts')
@click.option('--config_path',
              default='config.yaml',
              show_default=True,
              help='Path to config file.')
@click.option('--format', 'output_format',
              default='yaml',
              show_default=True,
              type=click.Choice(['yaml', 'json'], case_sensitive=False),
              )
def _node_counts(config_path, output_format):
    """Node counts in the sheepdog database by project."""
    assert config_path, '--config_path is required'
    assert pathlib.Path(config_path).exists(), f'--config_path {config_path} does not exist'
    dependency_order = yaml.safe_load(open(config_path))['dependency_order']
    dependency_order = [_ for _ in dependency_order if not _.startswith('_') and _ not in ['Project', 'Program']]
    dependency_order = [(rank + 1, _) for rank, _ in enumerate(dependency_order)]

    sql_selects = [
        f"select 0 as hierarchy_rank, 'node_project' as table, 'Project' as node, _props->>'code' as project, count(*) as count from node_project group by _props->>'code'"]
    for rank, _ in dependency_order:
        if _.startswith('_'):
            continue
        sql_selects.append(
            f"select {rank} as hierarchy_rank, 'node_{_.lower()}' as table, '{_}' as node , _props->>'project_id' as project, count(*) as count from node_{_.lower()} group by _props->>'project_id'")

    sql = "\nunion\n".join(sql_selects) + "ORDER BY hierarchy_rank;"
    conn = _connect_to_postgres()

    # check program/project exist
    cur = conn.cursor()
    cur.execute(sql)
    counts = cur.fetchall()
    counts = [{'hierarchy_rank': _[0], 'table': _[1], 'node': _[2], 'count': _[3]} for _ in counts]
    if output_format == 'yaml':
        yaml.dump(counts, sys.stdout, default_flow_style=False)
    else:
        json.dump(counts, sys.stdout, indent=2)


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
