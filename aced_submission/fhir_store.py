"""Adds, retrieves, and deletes FHIR resources in a FHIR store."""
import json
import pathlib
import sys

import click
import yaml
from elasticsearch import Elasticsearch

from aced_submission import NaturalOrderGroup
from aced_submission.meta_flat_load import read_ndjson, write_bulk_http, DEFAULT_ELASTIC


@click.group(cls=NaturalOrderGroup)
def fhir():
    """Adds, retrieves, and deletes FHIR resources in a FHIR store."""
    pass


def resource_generator(project_id, file_path):
    """Yield FHIR resources from a ndjson file."""
    program, project = project_id.split('-')
    assert program, "program is required"
    assert project, "project is required"

    for _ in read_ndjson(file_path):
        _['project_id'] = project_id
        _["auth_resource_path"] = f"/programs/{program}/projects/{project}"
        yield _

def fhir_put(project_id, path, elastic_url) -> list[str]:
    """Upsert FHIR resources to a FHIR store."""
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' separating program and project"

    elastic = Elasticsearch([elastic_url], request_timeout=120)

    index = doc_type ='fhir'
    limit = None
    logs = []
    for file_path in pathlib.Path(path).glob('*.ndjson'):

        write_bulk_http(elastic=elastic, index=index, doc_type=doc_type, limit=limit,
                        generator=resource_generator(project_id, file_path), schema=None)

        logs.append(f"wrote {file_path} to {elastic_url}/{index}")

    return logs


def fhir_get(project_id, path, elastic_url) -> list[str]:
    """Retrieve FHIR resources from FHIR store, write to path/resourceType.ndjson."""
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' separating program and project"
    program, project = project_id.split('-')
    assert program, "program is required"
    assert project, "project is required"

    elastic = Elasticsearch([elastic_url], request_timeout=120)

    index = doc_type = 'fhir'
    logs = []

    emitters = {}
    open_files = []


    def _emitter(_resource_type):
        """Maintain has of open files."""
        if resource_type not in emitters:
            file_path = pathlib.Path(path) / f"{_resource_type}.ndjson"
            emitters[_resource_type] = file_path.open('w')
            open_files.append(file_path)
        return emitters[_resource_type]

    auth_resource_path = f"/programs/{program}/projects/{project}"

    for _ in elastic.search(index=index, doc_type=doc_type,
                            q={"query": {"match": {"auth_resource_path": auth_resource_path}}}):
        resource_type = _['_source']['resourceType']
        file = _emitter(resource_type)
        json.dump(_['_source'], file)
        file.write('\n')

    for file in emitters.values():
        file.close()
    for file in open_files:
        logs.append(f"wrote {file}")

    return logs


@fhir.command(name='put')
@fhir.option('--project_id', required=True, show_default=True,
              help="Gen3 program-project")
@fhir.option('--format', 'output_format',
              default='yaml',
              show_default=True,
              type=click.Choice(['yaml', 'json'], case_sensitive=False))
@click.option('--elastic_url', default=DEFAULT_ELASTIC, show_default=True)
@fhir.argument('path', default=None, required=True, show_default=True)
def _fhir_put(project_id, output_format, path, elastic_url):
    """Upsert FHIR resources to a FHIR store."""
    logs = fhir_put(project_id, path, elastic_url)
    if output_format == 'yaml':
        yaml.dump(logs, sys.stdout, default_flow_style=False)
    else:
        json.dump(logs, sys.stdout, indent=2)



@fhir.command(name='get')
@fhir.option('--project_id', required=True, show_default=True,
              help="Gen3 program-project")
@fhir.option('--format', 'output_format',
             default='yaml',
             show_default=True,
             type=click.Choice(['yaml', 'json'], case_sensitive=False))
@click.option('--elastic_url', default=DEFAULT_ELASTIC, show_default=True)
@fhir.argument('path', default=None, required=True, show_default=True)
def _fhir_get(project_id, output_format, path, elastic_url):
    """Exports all resources for project_id to a directory."""
    logs = fhir_get(project_id, path, elastic_url)
    if output_format == 'yaml':
        yaml.dump(logs, sys.stdout, default_flow_style=False)
    else:
        json.dump(logs, sys.stdout, indent=2)
