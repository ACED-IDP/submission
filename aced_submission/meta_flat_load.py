"""Load flat indexes into elasticsearch."""


import csv
import json
import logging
import os
import pathlib
import sqlite3
import uuid
from datetime import datetime
from functools import lru_cache
from itertools import islice
from typing import Dict, Iterator

import click
import elasticsearch
import orjson
import requests
from dictionaryutils import DataDictionary
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)

DEFAULT_ELASTIC = "http://localhost:9200"

k8s_elastic = os.environ.get('GEN3_ELASTICSEARCH_MASTER_PORT', None)
if k8s_elastic:
    DEFAULT_ELASTIC = f"http://{k8s_elastic.replace('tcp://', '')}"

# TODO - fix me should be gen3.aced-idp.org but we need to coordinate with gitops.json
ES_INDEX_PREFIX = "gen3.aced.io"

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


def create_id(key: str) -> str:
    """Create an idempotent ID from the input string."""
    return str(uuid.uuid5(ACED_NAMESPACE, key))


def read_ndjson(path: str) -> Iterator[Dict]:
    """Read ndjson file, load json line by line."""
    with open(path) as jsonfile:
        for l_ in jsonfile.readlines():
            yield json.loads(l_)


def read_tsv(path: str) -> Iterator[Dict]:
    """Read tsv file line by line."""
    with open(path) as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter="\t")
        for row in reader:
            yield row


def create_index_from_source(_schema, _index, _type):
    """Given an ES source dict, create ES index."""
    mappings = {}
    if _type == 'file':
        # TODO fix me - we should have a index called document_reference not file
        properties = _schema['document_reference.yaml']['properties']
    else:
        properties = _schema[_type + '.yaml']['properties']
    for k, v in properties.items():
        if not isinstance(v, dict):
            continue
        if '$ref' in v:
            (ref_, ref_prop) = v['$ref'].split('#/')
            prop_type = _schema[ref_][ref_prop]['type']
        elif 'enum' in v:
            prop_type = 'string'
        elif 'oneOf' in v:
            prop_type = 'string'
        else:
            if 'type' not in v:
                print("?")
            if isinstance(v['type'], list):
                prop_type = v['type'][0]
            else:
                prop_type = v['type']

        if prop_type in ['string']:
            mappings[k] = {
                "type": "keyword"
            }
        elif prop_type in ['boolean']:
            mappings[k] = {
                "type": "keyword"
            }
        elif 'date' in prop_type:
            mappings[k] = {
                "type": "date"
            }
        elif 'array' in prop_type:
            mappings[k] = {
                "type": "keyword"
            }
        else:
            # naive, there are probably other types
            mappings[k] = {"type": "float"}
        # we have a patient centric index approach, all links include a `patient`
        mappings['patient_id'] = {"type": "keyword"}
        # patient fields copied to observation
        if _type == 'observation':
            mappings['us_core_race'] = {"type": "keyword"}
            mappings['address'] = {"type": "keyword"}
            mappings['gender'] = {"type": "keyword"}
            mappings['birthDate'] = {"type": "keyword"}
            mappings['us_core_ethnicity'] = {"type": "keyword"}
            mappings['address_orh_zip_designation_code'] = {"type": "keyword"}
            mappings['condition'] = {"type": "keyword"}
            mappings['condition_code'] = {"type": "keyword"}
            mappings['family_history_condition'] = {"type": "keyword"}
            mappings['family_history_condition_code'] = {"type": "keyword"}

    mappings['auth_resource_path'] = {"type": "keyword"}

    dynamic_templates = [
        {
            "strings": {
                "match_mapping_type": "string",
                "mapping": {
                    "type": "keyword"
                }
            }
        }
    ]

    return {
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html#dynamic-parameters
        "mappings": {"properties": mappings, 'dynamic_templates': dynamic_templates}
    }


def write_array_aliases(doc_type, alias, elastic=DEFAULT_ELASTIC, name_space=ES_INDEX_PREFIX):
    """Write the array aliases."""
    # EXPECTED_ALIASES = {
    #     ".kibana_1": {
    #         "aliases": {
    #             ".kibana": {}
    #         }
    #     },
    #     "etl-array-config_0": {
    #         "aliases": {
    #             "etl-array-config": {},
    #             "etl_array-config": {},
    #             "time_2022-08-25T01:44:47.115494": {}
    #         }
    #     },
    #     "etl_0": {
    #         "aliases": {
    #             "etl": {},
    #             "time_2022-08-25T01:44:47.115494": {}
    #         }
    #     },
    #     "file-array-config_0": {
    #         "aliases": {
    #             "file-array-config": {},
    #             "file_array-config": {},
    #             "time_2022-08-25T01:44:47.115494": {}
    #         }
    #     },
    #     "file_0": {
    #         "aliases": {
    #             "file": {},
    #             "time_2022-08-25T01:44:47.115494": {}
    #         }
    #     }
    # }
    return {
        "method": 'POST',
        "url": f'{elastic}/_aliases',
        "json": {
            "actions": [
                {"add": {"index": f"{name_space}_{doc_type}-array-config_0",
                         "alias": f"{name_space}_array-config"}},
                {"add": {"index": f"{name_space}_{doc_type}-array-config_0",
                         "alias": f"{alias}_array-config"}}
            ]}
    }


def write_array_config(doc_type, alias, field_array, elastic=DEFAULT_ELASTIC, name_space=ES_INDEX_PREFIX):
    """Write the array config."""
    return {
        "method": 'PUT',
        "url": f'/{name_space}_{doc_type}-array-config_0/_doc/{alias}',
        "json": {"timestamp": datetime.now().isoformat(), "array": field_array}
    }


def write_alias_config(doc_type, alias, elastic=DEFAULT_ELASTIC, name_space=ES_INDEX_PREFIX):
    """Write the alias config."""
    return {
        "method": 'POST',
        "url": f'{elastic}/_aliases',
        "json": {"actions": [{"add": {"index": f"{name_space}_{doc_type}_0", "alias": alias}}]}
    }


def create_indexes(_schema, _index, doc_type, elastic=DEFAULT_ELASTIC):
    """Create the es indexes."""
    return {
        "method": 'PUT',
        "url": f'{elastic}/{_index}',
        "json": create_index_from_source(_schema, _index, doc_type),
        "index": _index,
    }


def write_sqlite(index, generator):
    """Write to sqlite"""
    connection = sqlite3.connect(f'{index}.sqlite')
    with connection:
        connection.execute(f'DROP table IF EXISTS {index}')
        connection.execute(f'CREATE TABLE if not exists {index} (id PRIMARY KEY, entity Text)')
        with connection:
            connection.executemany(f'insert into {index} values (?, ?)',
                                   [(entity['id'], orjson.dumps(entity).decode(),) for entity in generator])


def write_bulk_http(elastic, index, limit, doc_type, generator, schema):
    """Use efficient method to write to elastic"""
    counter = 0

    def _bulker(generator_, counter_=counter):
        for dict_ in generator_:
            if limit and counter_ > limit:
                break  # for testing
            yield {
                '_index': index,
                '_op_type': 'index',
                '_source': dict_,
                # use the id from the FHIR object to upsert information
                '_id': dict_['id']
            }
            counter_ += 1
            if counter_ % 10000 == 0:
                logger.info(f"{counter_} records written")
        logger.info(f"{counter_} records written")

    if schema:
        logger.info(f'Creating {doc_type} indices.')
        index_dict = create_indexes(schema, _index=index, doc_type=doc_type)

        try:
            elastic.indices.create(index=index_dict['index'], body=index_dict['json'])
        except Exception as e:
            if 'resource_already_exists_exception' in str(e):
                logger.debug(f"Index already exists. {index} {str(e)}")
                logger.debug("Continuing to load.")
            else:
                raise e

    logger.info(f'Writing bulk to {index} limit {limit}.')
    _ = bulk(client=elastic,
             actions=(d for d in _bulker(generator)),
             request_timeout=120)


def observation_generator(project_id, path) -> Iterator[Dict]:
    """Render guppy index for observation."""
    program, project = project_id.split('-')

    connection = sqlite3.connect('denormalized_patient.sqlite')

    for observation in read_ndjson(path):
        o_ = observation['object']

        o_['project_id'] = project_id
        o_["auth_resource_path"] = f"/programs/{program}/projects/{project}"
        for relation in observation['relations']:
            dst_name = relation['dst_name'].lower()
            dst_id = relation['dst_id']
            o_[f'{dst_name}_id'] = dst_id

        #
        for required_field in []:
            if required_field not in o_:
                o_[required_field] = None

        assert 'patient_id' in o_, observation

        denormalized_patient = fetch_denormalized_patient(connection, o_['patient_id'])
        condition, condition_coding, fh_condition, fh_condition_coding, patient = (
            denormalized_patient['condition'],
            denormalized_patient['condition_coding'],
            denormalized_patient['fh_condition'],
            denormalized_patient['fh_condition_coding'],
            denormalized_patient['patient'],
        )

        if patient:
            o_['us_core_race'] = patient.get('us_core_race', None)
            o_['address'] = patient.get('address', None)
            o_['gender'] = patient.get('gender', None)
            o_['birthDate'] = patient.get('birthDate', None)
            o_['us_core_ethnicity'] = patient.get('us_core_ethnicity', None)
            o_['address_orh_zip_designation_code'] = patient.get('address_orh_zip_designation_code', None)

            o_['condition'] = condition
            o_['condition_code'] = condition_coding
            o_['family_history_condition'] = fh_condition
            o_['family_history_condition_code'] = fh_condition_coding

        yield o_


@lru_cache(maxsize=1024 * 10)
def fetch_denormalized_patient(connection, patient_id):
    """Retrieve unique conditions and family history"""

    fh_condition = []
    fh_condition_coding = []
    condition = []
    condition_coding = []
    patient = None

    for row in connection.execute('select entity from patient where id = ? limit 1', (patient_id,)):
        patient = orjson.loads(row[0])
        break

    for row in connection.execute('select entity from family_history where patient_id = ? ', (patient_id,)):
        family_history = orjson.loads(row[0])
        for _ in family_history['condition']:
            if _ not in fh_condition:
                fh_condition.append(_)
        for _ in family_history['condition_coding']:
            if _ not in fh_condition_coding:
                fh_condition_coding.append(_)

    for row in connection.execute('select entity from condition where patient_id = ? ', (patient_id,)):
        condition_ = orjson.loads(row[0])
        if condition_['code'] not in condition:
            condition.append(condition_['code'])
            condition_coding.append(condition_['code_coding'])

    return {
        'patient': patient, 'condition': condition, 'condition_coding': condition_coding,
        'fh_condition': fh_condition, 'fh_condition_coding': fh_condition_coding
    }


def patient_generator(project_id, path) -> Iterator[Dict]:
    """Render guppy index for patient."""
    program, project = project_id.split('-')
    for patient in read_ndjson(path):
        p_ = patient['object']
        p_['id'] = patient['id']

        p_['project_id'] = project_id
        p_["auth_resource_path"] = f"/programs/{program}/projects/{project}"

        #
        for required_field in []:
            if required_field not in p_:
                p_[required_field] = None
        yield p_


def file_generator(project_id, path) -> Iterator[Dict]:
    """Render guppy index for file."""
    program, project = project_id.split('-')
    for file in read_ndjson(path):
        f_ = file['object']
        f_['id'] = file['id']

        f_['project_id'] = project_id
        f_["auth_resource_path"] = f"/programs/{program}/projects/{project}"

        for relation in file['relations']:
            dst_name = relation['dst_name'].lower()
            dst_id = relation['dst_id']
            f_[f'{dst_name}_id'] = dst_id

        #
        for required_field in []:
            if required_field not in f_:
                f_[required_field] = None
        yield f_


def setup_aliases(alias, doc_type, elastic, field_array, index):
    """Create the alias to the data index"""
    elastic.indices.put_alias(index, alias)
    # create a configuration index that guppy will read that describes the array fields
    # TODO - find a doc or code reference in guppy that explains how this is used
    alias_index = f'{ES_INDEX_PREFIX}_{doc_type}-array-config_0'
    try:
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "array": {"type": "keyword"},
                }
            }
        }
        elastic.indices.create(index=alias_index, body=mapping)
    except Exception as e:
        logger.warning(f"Could not create index. {index} {str(e)}")
        logger.warning("Continuing to load.")

    try:
        elastic.create(alias_index, id=alias,
                       body={"timestamp": datetime.now().isoformat(), "array": field_array})
    except elasticsearch.exceptions.ConflictError:
        pass
    elastic.indices.update_aliases(
        {"actions": [{"add": {"index": f"{ES_INDEX_PREFIX}_{doc_type}_0", "alias": alias}}]}
    )
    elastic.indices.update_aliases({
        "actions": [
            {"add": {"index": f"{ES_INDEX_PREFIX}_{doc_type}-array-config_0",
                     "alias": f"{ES_INDEX_PREFIX}_array-config"}},
            {"add": {"index": f"{ES_INDEX_PREFIX}_{doc_type}-array-config_0",
                     "alias": f"{doc_type}_array-config"}}
        ]}
    )


@click.group('flat')
def cli():
    """Load flat indexes into elasticsearch."""
    pass


def write_flat_file(output_path, index, doc_type, limit, generator, schema):
    """Write the flat model to a file."""
    counter_ = 0

    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    with open(f"{output_path}/{doc_type}.ndjson", "wb") as fp:
        for dict_ in generator:
            fp.write(
                orjson.dumps(
                    {
                        'id': dict_['id'],
                        'object': dict_,
                        'name': doc_type,
                        'relations': []
                    }
                )
            )
            fp.write(b'\n')

            counter_ += 1
            if counter_ % 10000 == 0:
                logger.info(f"{counter_} records written")
        logger.info(f"{counter_} records written")


@cli.command('denormalize-patient')
@click.option('--input_path', required=True,
              default=None,
              show_default=True,
              help='Path to flattened json'
              )
def _denormalize_patient(input_path):
    denormalize_patient(input_path)


def denormalize_patient(input_path):
    """Gather Patient, FamilyHistory, Condition into sqlite db."""

    path = pathlib.Path(input_path)

    def _load_vertex(file_name):
        """Get the object and patient id"""
        if not (path / file_name).is_file():
            return
        for _ in read_ndjson(path / file_name):
            patient_id = None
            if len(_['relations']) == 1 and _['relations'][0]['dst_name'] == 'Patient':
                patient_id = _['relations'][0]['dst_id']
            _ = _['object']
            _['id'] = _['id']
            if patient_id:
                _['patient_id'] = patient_id
            yield _

    connection = sqlite3.connect('denormalized_patient.sqlite')
    with connection:
        connection.execute('DROP table IF EXISTS patient')
        connection.execute('DROP table IF EXISTS family_history')
        connection.execute('DROP table IF EXISTS condition')
        connection.execute('CREATE TABLE if not exists patient (id PRIMARY KEY, entity Text)')
        connection.execute('CREATE TABLE if not exists family_history (id PRIMARY KEY, patient_id Text, entity Text)')
        connection.execute('CREATE TABLE if not exists condition (id PRIMARY KEY, patient_id Text, entity Text)')
    with connection:
        connection.executemany('insert into patient values (?, ?)',
                               [(entity['id'], orjson.dumps(entity).decode(),) for entity in
                                _load_vertex('Patient.ndjson')])
    with connection:
        connection.executemany('insert into family_history values (?, ?, ?)',
                               [(entity['id'], entity['patient_id'], orjson.dumps(entity).decode(),) for entity in
                                _load_vertex('FamilyMemberHistory.ndjson')])
    with connection:
        connection.executemany('insert into condition values (?, ?, ?)',
                               [(entity['id'], entity['patient_id'], orjson.dumps(entity).decode(),) for entity in
                                _load_vertex('Condition.ndjson')])
    with connection:
        connection.execute('CREATE INDEX if not exists condition_patient_id on condition(patient_id)')
        connection.execute('CREATE INDEX if not exists family_history_patient_id on condition(patient_id)')


@cli.command('load')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='program-project'
              )
@click.option('--index', required=True,
              default=None,
              show_default=True,
              help='Elastic index name'
              )
@click.option('--path', required=True,
              default=None,
              show_default=True,
              help='Path to flattened json'
              )
@click.option('--elastic_url', default=DEFAULT_ELASTIC, show_default=True)
@click.option('--limit',
              default=None,
              show_default=True,
              help='Max number of rows per index.')
@click.option('--schema_path', required=True,
              default='generated-json-schema/aced.json',
              show_default=True,
              help='Path to gen3 schema json'
              )
@click.option('--output_path', required=False,
              default=None,
              show_default=True,
              help='Do not load elastic, write flat model to file instead'
              )
def _load_flat(project_id, index, path, limit, elastic_url, schema_path, output_path):
    """Gen3 Elastic Search data into guppy (patient, observation, files, etc.)."""
    load_flat(project_id, index, path, limit, elastic_url, schema_path, output_path)


def load_flat(project_id, index, path, limit, elastic_url, schema_path, output_path):
    # replaces tube_lite

    if limit:
        limit = int(limit)

    elastic = Elasticsearch([elastic_url], request_timeout=120)

    index = index.lower()

    if 'http' in schema_path:
        schema = requests.get(schema_path).json()
    else:
        schema = DataDictionary(local_file=schema_path).schema

    if index == 'patient':
        doc_type = 'patient'
        index = f"{ES_INDEX_PREFIX}_{doc_type}_0"
        alias = 'patient'
        field_array = [k for k, v in schema['patient.yaml']['properties'].items() if 'array' in v.get('type', {})]

        if not output_path:
            # create the index and write data into it.
            write_bulk_http(elastic=elastic, index=index, doc_type=doc_type, limit=limit,
                            generator=patient_generator(project_id, path), schema=schema)

            setup_aliases(alias, doc_type, elastic, field_array, index)
        else:
            # write file path
            write_flat_file(output_path=output_path, index=index, doc_type=doc_type, limit=limit,
                            generator=patient_generator(project_id, path), schema=schema)

    if index == 'observation':
        doc_type = 'observation'
        index = f"{ES_INDEX_PREFIX}_{doc_type}_0"
        alias = 'observation'
        field_array = [k for k, v in schema['observation.yaml']['properties'].items() if 'array' in v.get('type', {})]
        # field_array = ['data_format', 'data_type', '_file_id', 'medications', 'conditions']

        if not output_path:
            # create the index and write data into it.
            write_bulk_http(elastic=elastic, index=index, doc_type=doc_type, limit=limit,
                            generator=observation_generator(project_id, path), schema=schema)

            setup_aliases(alias, doc_type, elastic, field_array, index)
        else:
            # write file path
            write_flat_file(output_path=output_path, index=index, doc_type=doc_type, limit=limit,
                            generator=observation_generator(project_id, path), schema=schema)

    if index == 'file':
        doc_type = 'file'
        alias = 'file'
        index = f"{ES_INDEX_PREFIX}_{doc_type}_0"
        field_array = [k for k, v in schema['document_reference.yaml']['properties'].items() if
                       isinstance(v, dict) and 'array' in v.get('type',
                                                                {})]
        if not output_path:
            # create the index and write data into it.
            write_bulk_http(elastic=elastic, index=index, doc_type=doc_type, limit=limit,
                            generator=file_generator(project_id, path), schema=schema)

            setup_aliases(alias, doc_type, elastic, field_array, index)
        else:
            # write file path
            write_flat_file(output_path=output_path, index=index, doc_type=doc_type, limit=limit,
                            generator=file_generator(project_id, path), schema=schema)


def chunk(arr_range, arr_size):
    """Iterate in chunks."""
    arr_range = iter(arr_range)
    return iter(lambda: tuple(islice(arr_range, arr_size)), ())


@cli.command('counts')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='program-project'
              )
def _counts(project_id):
    counts(project_id)


def counts(project_id):
    """Count the number of patients, observations, and files."""
    elastic = Elasticsearch([DEFAULT_ELASTIC], request_timeout=120)
    program, project = project_id.split('-')
    assert program, "program is required"
    assert project, "project is required"
    query = {
        "query": {
            "match": {
                "auth_resource_path": f"/programs/{program}/projects/{project}"
            }
        }
    }
    for index in ['patient', 'observation', 'file']:
        # index = f"{ES_INDEX_PREFIX}_{index}_0"
        print(index, elastic.count(index=index, body=query)['count'])


@cli.command('rm')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='program-project'
              )
@click.option('--index', required=True,
              default=None,
              show_default=True,
              help='one of patient, observation, file'
              )
def _delete(project_id, index):
    delete(project_id, index)


def delete(project_id, index):
    """Delete items from elastic index for project_id."""
    elastic = Elasticsearch([DEFAULT_ELASTIC], request_timeout=120)
    assert project_id, "project_id is required"
    program, project = project_id.split('-')
    assert program, "program is required"
    assert project, "project is required"
    assert index, "index is required"
    query = {
        "query": {
            "match": {
                "auth_resource_path": f"/programs/{program}/projects/{project}"
            }
        }
    }
    print("deleting, waiting up to 5 min. for response")
    print(index, elastic.delete_by_query(index=index, body=query, timeout='5m'))


if __name__ == '__main__':
    cli()
