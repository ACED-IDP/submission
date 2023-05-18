import gzip
import io
import json
import pathlib
import threading
import uuid

import click
import orjson
from fhir.resources import FHIRAbstractModel  # noqa
from fhir.resources.bundle import Bundle
from pydantic import ValidationError

from aced_submission import NaturalOrderGroup
from aced_submission.dir_to_study.transform import cli as dir_to_study
from aced_submission.simplifier import cli as simplifier
from aced_submission.util import directory_reader, parse_obj
from aced_submission.gen_validator import directory_reader as gen3_directory_reader


LINKS = threading.local()
CLASSES = threading.local()
IDENTIFIER_LIST_SIZE = 8

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


@click.group(cls=NaturalOrderGroup)
def meta():
    """Project data (ResearchStudy, ResearchSubjects, Patient, etc.)."""
    pass


meta.add_command(dir_to_study)
meta.add_command(simplifier)


@meta.command('validate')
@click.option('--path', required=True, default=None, show_default=True,
              help='Path to FHIR files.')
@click.option('--pattern', required=True, default="*.*", show_default=True,
              help='File name pattern')
def _validate(path, pattern):
    """Check FHIR data for validity and ACED conventions."""
    ok = True
    for result in directory_reader(pathlib.Path(path), pattern):
        if result.exception:
            ok = False
            print('file:', result.path)
            print('\toffset:', result.offset)
            print('\tresource_id:', result.resource_id)
            msg = str(result.exception).replace('\n', '\n\t\t')
            print('\texception:', msg)
    if ok:
        print('OK, all resources pass')


@meta.command('validate-gen3')
@click.option('--path', required=True, default=None, show_default=True,
              help='Path to Gen3 ndjson files.')
@click.option('--schema_path', required=True,
              default='generated-json-schema/aced.json',
              show_default=True,
              help='Path to gen3 schema json, a file path or url'
              )
def _validate_gen3(path, schema_path):
    """Check simplified Gen3 data for validity and ACED conventions."""
    ok = True
    for result in gen3_directory_reader(pathlib.Path(path), schema_path):
        if result.exception:
            ok = False
            print('file:', result.path)
            print('\toffset:', result.offset)
            print('\tresource_id:', result.resource_id)
            msg = str(result.exception).replace('\n', '\n\t\t')
            print('\texception:', msg)
    if ok:
        print('OK, all resources pass')


@meta.command('migrate')
@click.option('--input_path', required=True,
              default=None,
              show_default=True,
              help='Path containing bundles (*.json) or resources (*.ndjson)'
              )
@click.option('--output_path', required=True,
              default=None,
              show_default=True,
              help='Path where migrated resources will be stored'
              )
@click.option('--validate', default=False, is_flag=True, show_default=True,
              help="Validate after migration")
def migrate(input_path, output_path, validate):
    """Migrate from FHIR R4B to R5.0"""

    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)
    assert input_path.is_dir(), input_path
    assert output_path.is_dir(), output_path

    for input_file in input_path.glob('*.json'):
        with open(input_file, "rb") as fp:
            bundle_ = orjson.loads(fp.read())
            if 'entry' not in bundle_:
                print(f"No 'entry' in bundle {input_file} ")
                break
        for entry in bundle_['entry']:
            resource = entry['resource']
            _ = _migrate_resource(resource, validate)

        if validate:
            _ = Bundle.parse_obj(bundle_)

        output_file = output_path / input_file.name
        with open(output_file, "wb") as fp:
            fp.write(orjson.dumps(bundle_))
        print('migrate', input_file, output_file)

    for input_file in input_path.glob('*.ndjson'):
        with open(input_file, "r") as fp:
            output_file = output_path / input_file.name
            print('migrate', input_file, output_file)
            with open(output_file, "wb") as out_fp:

                for line in fp.readlines():
                    resource = orjson.loads(line)
                    _ = _migrate_resource(resource, validate)
                    out_fp.write(orjson.dumps(_, option=orjson.OPT_APPEND_NEWLINE))

    for input_file in input_path.glob('*.json.gz'):

        with io.TextIOWrapper(io.BufferedReader(gzip.GzipFile(input_file))) as fp:
            output_file = output_path / input_file.name
            print('migrate', input_file, output_file)
            with gzip.open(output_file, 'wb') as out_fp:
                for line in fp.readlines():
                    resource = orjson.loads(line)
                    try:
                        _ = _migrate_resource(resource, validate)
                        out_fp.write(orjson.dumps(_, option=orjson.OPT_APPEND_NEWLINE))
                    except Exception as e:
                        print('\t', str(e))
                        break


def _migrate_resource(resource, validate):
    """Apply migrations"""
    #
    # xform all bundles to 5.0 see https://build.fhir.org/<lower-case-resource-name>
    # from https://hl7.org/fhir/r4b/<lower-case-resource-name>
    #
    assert 'resourceType' in resource, ('missing resourceType', orjson.dumps(resource).decode())

    resource_type = resource['resourceType']

    if resource_type == "Encounter":
        resource['class'] = [
            {
                'coding': [resource['class']]
            }
        ]
        for _ in resource['participant']:
            _['actor'] = _['individual']
            del _['individual']
        resource['actualPeriod'] = resource['period']
        del resource['period']
        if 'reasonCode' in resource:
            resource['reason'] = [{'use': resource['reasonCode']}]
            del resource['reasonCode']
        if 'hospitalization' in resource:
            resource['admission'] = resource['hospitalization']
            del resource['hospitalization']

    if resource_type == "DocumentReference":
        for _ in resource['content']:
            if 'format' in _:
                del _['format']
        if 'context' in resource and 'encounter' in resource['context']:
            del resource['context']['period']
            resource['context'] = resource['context']['encounter']
        if 'context' in resource and 'related' in resource['context']:
            resource['subject'] = resource['context']['related'][0]
            del resource['context']

    if resource_type == "Observation":
        _ = resource.get('valueSampledData', None)
        if _:
            _['intervalUnit'] = '/s'
            _['interval'] = _['period']
            del _['period']

    if resource_type == "MedicationAdministration":
        resource['occurenceDateTime'] = resource['effectiveDateTime']
        del resource['effectiveDateTime']

        resource['medication'] = {
            'concept': resource['medicationCodeableConcept']
        }
        del resource['medicationCodeableConcept']

        resource['encounter'] = resource['context']
        del resource['context']

        if 'reasonReference' in resource:
            resource['reason'] = [{'reference': _} for _ in resource['reasonReference']]
            del resource['reasonReference']

    if resource_type == "ResearchSubject":
        if 'individual' in resource:
            resource['subject'] = resource['individual']
            del resource['individual']

    if validate:
        try:
            _ = parse_obj(resource)
        except ValidationError as e:
            print('ValidationError', str(e), json.dumps(resource))
            raise e
    return resource


if __name__ == '__main__':
    meta()
