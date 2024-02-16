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
from aced_submission.meta_discovery_load import discovery_load, discovery_delete, discovery_get

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
@click.option('--project_id', required=True, show_default=True,
              help='Gen3 program-project')
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
def _meta_upload(source_path, project_id, silent, dictionary_path, config_path):
    """Copy simplified json into sheepdog database."""
    program, project = project_id.split('-')
    assert program, "program is required"
    assert project, "project is required"

    meta_upload(source_path, program, project, silent, dictionary_path, config_path,
                file_name_pattern='**/*.ndjson')


@graph.command(name='mapping')
@click.option('--dictionary_path',
              default='https://aced-public.s3.us-west-2.amazonaws.com/aced.json',  # 'output/gen3',
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
        "select 0 as hierarchy_rank, 'node_project' as table, 'Project' as node, _props->>'code' as project, count(*) as count from node_project group by _props->>'code'"]
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
    counts = [{'hierarchy_rank': _[0], 'table': _[1], 'node': _[2], 'project_id': _[3], 'count': _[4]} for _ in counts]
    if output_format == 'yaml':
        yaml.dump(counts, sys.stdout, default_flow_style=False)
    else:
        json.dump(counts, sys.stdout, indent=2)


@graph.command(name='rm')
@click.option('--config_path',
              default='config.yaml',
              show_default=True,
              help='Path to config file.')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='program-project'
              )
@click.option('--format', 'output_format',
              default='yaml',
              show_default=True,
              type=click.Choice(['yaml', 'json'], case_sensitive=False),
              )
def _graph_rm(config_path, project_id, output_format):
    """Remove records from the sheepdog database by project."""
    assert config_path, '--config_path is required'
    assert pathlib.Path(config_path).exists(), f'--config_path {config_path} does not exist'

    program, project = project_id.split('-')
    assert program, "program is required"
    assert project, "project is required"

    reverse_dependency_order = yaml.safe_load(open(config_path))['dependency_order']
    reverse_dependency_order = [_ for _ in reverse_dependency_order if not _.startswith('_') and _ not in ['Project', 'Program']]
    reverse_dependency_order = [(rank + 1, _) for rank, _ in enumerate(reverse_dependency_order)]
    reverse_dependency_order.reverse()

    results = []
    conn = _connect_to_postgres()
    with conn:
        with conn.cursor() as curs:
            for rank, _ in reverse_dependency_order:
                if _.startswith('_'):
                    continue
                curs.execute(f"delete from node_{_.lower()} where _props->>'project_id' = %s;", (project_id,))
                results.append({'table': f'node_{_.lower()}', 'project_id': project_id, 'count': curs.rowcount})
            curs.execute("delete from node_project where _props->>'code' = %s;", (project,))
            results.append({'table': 'node_project', 'project_id': project, 'count': curs.rowcount})

    if output_format == 'yaml':
        yaml.dump(results, sys.stdout, default_flow_style=False)
    else:
        json.dump(results, sys.stdout, indent=2)


meta.add_command(meta_flat_load_cli)


@meta.group(name='discovery')
def discovery():
    """Gen3 discovery database."""
    pass


@discovery.command('load')
@click.option('--project_id', required=True,
               help='The {program}-{project} project identifier for the study')
@click.option('--subjects_count', required=True,
               help='The number of subjects in the study.')
@click.option('--description', required=True,
               help='A summary description of the study.')
@click.option('--location', required=True,
               help='A url of a reference website associated with the study')
def _discovery_load(project_id, subjects_count, description, location):
    """Writes project information to discovery metadata-service"""
    discovery_load(project_id, subjects_count, description, location)


@discovery.command('delete')
@click.option('--project_id', required=True,
               help='A url of a reference website associated with the study')
def _discovery_delete(project_id):
    """Deletes project information from discovery metadata-service"""
    discovery_delete(project_id)

@discovery.command('get')
@click.option('--project_id', required=True,
    help='A url of a reference website associated with the study')
def _discovery_get(project_id):
    """Fetches project information from discovery metadata-service"""
    discovery_get(project_id)


if __name__ == '__main__':
    meta()
