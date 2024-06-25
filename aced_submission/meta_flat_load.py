"""Load flat indexes into elasticsearch."""


import csv
import json
import logging
import os
import pathlib
import sqlite3
import tempfile
import uuid
from datetime import datetime
from functools import lru_cache
from itertools import islice
from typing import Dict, Iterator, Any, Generator, List

import click
from dateutil.parser import parse
from dateutil import tz
from gen3_tracker.meta.dataframer import LocalFHIRDatabase
import orjson
from opensearchpy import OpenSearch as Elasticsearch
from opensearchpy.helpers import bulk

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


def generate_elasticsearch_mapping(df: List[Dict]) -> Dict[str, Any]:
    """
    Generates an Elasticsearch mapping from a "DataFrame".

    Args:
        df (Dict): A list of dict.

    Returns:
        Dict[str, Any]: The generated Elasticsearch mapping.
    """

    def is_integer_dtype(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    def is_float_dtype(value: Any) -> bool:
        return isinstance(value, float)

    def is_bool_dtype(value: Any) -> bool:
        return isinstance(value, bool)

    def is_datetime64_any_dtype(value: Any) -> bool:
        try:
            if not isinstance(value, str):
                raise ValueError('Value is not a string')
            tzinfos = {"PDT": tz.gettz('US/West')}
            parse(value, fuzzy=True, tzinfos=tzinfos)
            return True
        except Exception:  # noqa
            return False

    def is_object_dtype(value: Any) -> bool:
        return isinstance(value, list) or isinstance(value, dict)

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
    mapping = {"mappings": {"properties": {}, 'dynamic_templates': dynamic_templates}}
    # see https://github.com/uc-cdis/guppy/blob/f5bb705dae6f3417e471ba2e43ce4b61ba5026fb/src/server/schema.js#L4
    # for the guppy schema types
    for row in df:
        for column in row.keys():
            if is_integer_dtype(row[column]):
                mapping["mappings"]["properties"][column] = {"type": "integer"}
            elif is_float_dtype(row[column]):
                mapping["mappings"]["properties"][column] = {"type": "float"}
            elif is_bool_dtype(row[column]):
                mapping["mappings"]["properties"][column] = {"type": "keyword"}
            elif is_datetime64_any_dtype(row[column]):
                mapping["mappings"]["properties"][column] = {"type": "keyword"}
            elif is_object_dtype(row[column]):
                if isinstance(row[column], list):
                    mapping["mappings"]["properties"][column] = {"type": "keyword"}
                else:
                    mapping["mappings"]["properties"][column] = {"type": "keyword"}
            elif isinstance(row[column], str):
                mapping["mappings"]["properties"][column] = {"type": "keyword"}
    return mapping


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


def write_bulk_http(elastic, index, limit, doc_type, generator) -> None:
    """Use efficient method to write to elastic, assumes a)generator is a list of dictionaries b) indices already exist. """
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

    logger.info(f'Writing bulk to {index} limit {limit}.')
    _ = bulk(client=elastic,
                       actions=(d for d in _bulker(generator)),
                       request_timeout=120,
                       max_retries=5)

    return


def observation_generator(project_id, generator) -> Iterator[Dict]:
    """Render guppy index for observation."""
    program, project = project_id.split('-')
    for observation in generator:
        observation['project_id'] = project_id
        observation["auth_resource_path"] = f"/programs/{program}/projects/{project}"
        yield observation


def file_generator(project_id, generator) -> Iterator[Dict]:
    """Render guppy index for file."""
    program, project = project_id.split('-')
    for file in generator:
        file['project_id'] = project_id
        file["auth_resource_path"] = f"/programs/{program}/projects/{project}"
        yield file


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


def setup_aliases(alias, doc_type, elastic, field_array, index):
    """Create the alias to the data index"""
    if not elastic.indices.exists_alias(alias):
        logger.warning(f"Creating alias {alias}.")
        elastic.indices.put_alias(index, alias)
    else:
        logger.info(f"Alias {alias} already exists.")
    # create a configuration index that guppy will read that describes the array fields
    # TODO - find a doc or code reference in guppy that explains how this is used
    array_config_index = f'{ES_INDEX_PREFIX}_{doc_type}-array-config_0'
    try:
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "array": {"type": "keyword"},
                }
            }
        }
        if not elastic.indices.exists(index=array_config_index):
            logger.warning(f"Creating index {array_config_index}.")
            elastic.indices.create(index=array_config_index, body=mapping)

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
            logger.warning(f"Updated aliases {array_config_index}")
        else:
            logger.warning(f"{array_config_index} already exists.")
        elastic.index(index=array_config_index, id=alias,
                      body={"timestamp": datetime.now().isoformat(), "array": field_array},
                      refresh='wait_for')
        logger.warning(f"Populated {array_config_index} field_array {field_array}")

    except Exception as e:
        logger.warning(f"Could not create index. {array_config_index} {str(e)}")
        logger.warning("Continuing to load.")


@click.group('flat')
def cli():
    """Load flat indexes into elasticsearch."""
    pass


def write_flat_file(output_path, index, doc_type, limit, generator):
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


def compare_mapping(existing_mapping: Dict[str, Any], new_mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares an existing Elasticsearch index mapping with a new mapping create update for the index by adding missing fields.

    Args:
        existing_mapping (Dict[str, Any]): The existing mapping.
        new_mapping (Dict[str, Any]): The new mapping to compare against the existing mapping.

    Returns:
        None
    """

    new_properties = new_mapping['mappings']['properties']
    existing_properties = existing_mapping['mappings']['properties']

    # Find differences and update mapping
    updates = {}
    for field, field_type in new_properties.items():
        if field not in existing_properties:
            updates[field] = field_type

    return updates


def ndjson_file_generator(path):
    """Read ndjson file line by line."""
    with open(path) as f:
        for l_ in f.readlines():
            yield orjson.loads(l_)


@cli.command('load')
@click.option('--input_path', required=True,
              default='META/',
              show_default=True,
              help='Path to flattened json'
              )
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='program-project'
              )
@click.option('--data_type', required=True,
              default='observation',
              type=click.Choice(['observation', 'file'], case_sensitive=False),
              show_default=True,
              help='index to load[observation, file] '
              )
def _load_flat(input_path, project_id, data_type):
    import tempfile

    work_path = tempfile.TemporaryDirectory(delete=False).name
    assert pathlib.Path(work_path).exists(), f"Directory {work_path} does not exist."
    work_path = pathlib.Path(work_path)
    db_path = (work_path / "local_fhir.db")
    db_path.unlink(missing_ok=True)

    db = LocalFHIRDatabase(db_name=db_path)
    db.load_ndjson_from_dir(path=input_path)

    load_flat(project_id=project_id,
              generator=db.flattened_observations(),
              index=data_type,
              limit=None,
              elastic_url=DEFAULT_ELASTIC,
              output_path=None
              )


def load_flat(project_id: str, index: str, generator: Generator[dict, None, None], limit: str, elastic_url: str, output_path: str):
    """Loads flattened FHIR data into Elasticsearch database. Replaces tube-lite"""

    assert index in ["observation", "file"], f"Index {index} does not have a supported generator"

    if limit:
        limit = int(limit)

    elastic = Elasticsearch([elastic_url], request_timeout=120, max_retries=5)
    assert elastic.ping(), f"Connection to {elastic_url} failed"
    index = index.lower()

    def load_index(elastic: Elasticsearch, output_path: str, es_index: str, alias: str, doc_type: str, limit: int):
        if not output_path:
            # create the index and write data into it.

            # since we need to read the generator twice, once to create the indices and once to write the data to ES
            # Get the path of the temporary file
            temp_path = tempfile.NamedTemporaryFile(delete=False).name
            # just write the data to it
            with open(temp_path, mode='w') as f:
                for _ in generator:
                    f.write(orjson.dumps(_).decode())
                    f.write('\n')

            if index == "observation":
                loading_generator = observation_generator(project_id, ndjson_file_generator(temp_path))
            elif index == "file":
                loading_generator = file_generator(project_id, ndjson_file_generator(temp_path))

            if elastic.indices.exists(index=es_index):
                logger.info(f"Index {es_index} exists.")

                existing_mapping = elastic.indices.get_mapping(index=es_index)
                assert es_index in existing_mapping, f"doc_type {es_index} not in {existing_mapping}"
                existing_mapping = existing_mapping[es_index]

                new_mapping = generate_elasticsearch_mapping(ndjson_file_generator(temp_path))
                updates = compare_mapping(existing_mapping, new_mapping)
                if updates != {}:
                    update_body = {
                        "properties": updates
                    }
                    elastic.indices.put_mapping(index=es_index, body=update_body)
                    logger.info(f"Updated {es_index} with {updates}")
                else:
                    logger.info(f"No updates needed for {es_index}")
            else:
                logger.info(f"Index {es_index} does not exist.")
                mapping = generate_elasticsearch_mapping(ndjson_file_generator(temp_path))
                elastic.indices.create(index=es_index, body=mapping)
                logger.info(f"Created {es_index}")

            write_bulk_http(elastic=elastic, index=es_index, doc_type=doc_type, limit=limit,
                            generator=loading_generator)

            field_array = set()
            for _ in ndjson_file_generator(temp_path):
                field_array.update([k for k, v in _.items() if isinstance(v, list)])
            field_array = list(field_array)
            setup_aliases(alias, doc_type, elastic, field_array, es_index)

            pathlib.Path(temp_path).unlink()

        else:
            # write file path
            write_flat_file(output_path=output_path, index=es_index, doc_type=doc_type, limit=limit,
                            generator=loading_generator)

    if index == 'observation':
        doc_type = 'observation'
        load_index(elastic=elastic, output_path=output_path, es_index=f"{ES_INDEX_PREFIX}_{doc_type}_0", alias="observation", doc_type=doc_type, limit=limit)

    elif index == 'file':
        doc_type = 'file'
        load_index(elastic=elastic, output_path=output_path, es_index=f"{ES_INDEX_PREFIX}_{doc_type}_0", alias="file", doc_type=doc_type, limit=limit)


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
    elastic = Elasticsearch([DEFAULT_ELASTIC], request_timeout=120, max_retries=5)
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
    for index in ['observation', 'file']:
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
              help='one of observation, file'
              )
def _delete(project_id, index):
    delete(project_id, index)


def delete(project_id, index):
    """Delete items from elastic index for project_id."""
    elastic = Elasticsearch([DEFAULT_ELASTIC], request_timeout=120, max_retries=5)
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
