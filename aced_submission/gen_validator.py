import logging
import pathlib
from typing import Iterator

import fastjsonschema
import inflection
import orjson
import requests
from dictionaryutils import DataDictionary
from jsonschema.exceptions import ValidationError

from aced_submission.util import _to_file, ParseResult

logger = logging.getLogger(__name__)

LOGGED_ALREADY = []
COMPILED_SCHEMAS = {}


def log_once(msg):
    if msg not in LOGGED_ALREADY:
        logger.info(msg)
        LOGGED_ALREADY.append(msg)


def _validate(gen3_resource: dict, schemas: dict):
    """Ensure resource is valid for gen3 schema."""
    try:
        resource = None
        for expected_property in ['id', 'relations', 'object']:
            assert expected_property in gen3_resource, f"{expected_property} not in {gen3_resource}"

        resource = gen3_resource['object']
        resource_type = resource['resourceType']
        key = inflection.underscore(resource_type)
        schema = schemas.get(key, schemas.get(f"{key}.yaml", None))
        assert schema, f"Could not find schema for {key}"
        actual_keys = set(resource.keys())
        expected_keys = set(schema['properties'].keys()).union(set([_['name'] for _ in schema['links']]))
        if not actual_keys.issubset(expected_keys):
            if not schema.get('additionalProperties', False):
                assert False, f"Is not a subset {actual_keys - expected_keys} not expected"
            else:
                for _ in actual_keys - expected_keys:
                    msg = f"extra property: {resource_type}.{_} {type(resource[_]).__name__}"
                    log_once(msg)
        if 'required' in schema:
            for _ in schema['required']:
                assert _ in resource, f"{_} missing {resource}"
        compiled_schema = COMPILED_SCHEMAS.get(key, None)
        if not compiled_schema:
            compiled_schema = fastjsonschema.compile(
                schema,
                formats={
                    'time': r'^(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$',
                    'date': r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])?$',
                    'binary': r'^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$',
                    "date-time": r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$',
                    "uri": r'\w+:(\/?\/?)[^\s]+',
                    "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$"
                }
            )
            COMPILED_SCHEMAS[key] = compiled_schema
        compiled_schema(resource)
        return ParseResult(resource=resource, exception=None, path=None, resource_id=resource.get('id', None))
    except (ValidationError, AssertionError) as e:
        if resource:
            return ParseResult(resource=None, exception=e, path=None, resource_id=resource.get('id', None))
        else:
            return ParseResult(resource=None, exception=e, path=None)


def directory_reader(
        directory_path: pathlib.Path,
        schema_path: str,
        pattern: str = '*.ndjson',
        validate=True) -> Iterator[ParseResult]:
    """Extract FHIR resources from directory"""

    assert directory_path.is_dir(), f"{directory_path.name} is not a directory"

    if 'http' in schema_path:
        schemas = requests.get(schema_path).json()
    else:
        schemas = DataDictionary(local_file=schema_path).schema

    input_files = [_ for _ in directory_path.glob(pattern)]
    for input_file in input_files:
        logger.info(input_file)
        if not input_file.is_file():
            continue
        fp = _to_file(input_file)
        with fp:
            offset = 0
            for line in fp.readlines():
                gen3_resource = orjson.loads(line)
                parse_result = _validate(gen3_resource, schemas=schemas)
                parse_result.path = input_file
                parse_result.offset = offset
                offset += 1
                yield parse_result
