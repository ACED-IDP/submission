import gzip
import importlib
import io
import pathlib
from dataclasses import dataclass
from typing import Dict, Iterator

import orjson
from fhir.resources.fhirresourcemodel import FHIRResourceModel
from pydantic import ValidationError
import logging

FHIR_CLASSES = importlib.import_module('fhir.resources')

logger = logging.getLogger(__name__)


@dataclass
class ParseResult:
    """Results of FHIR validation of dict."""
    resource: FHIRResourceModel
    """If valid, the FHIR resource."""
    exception: Exception
    """If invalid, the exception."""
    path: pathlib.Path = None
    """Source file, if available."""
    offset: int = None
    """Base 0 offset of line number(ndjson) or entry(bundle)."""
    resource_id: str = None
    """Resource id of resource"""


def parse_obj(resource: Dict, validate=True) -> ParseResult:
    """Load a dictionary into a FHIR model """
    try:
        assert 'resourceType' in resource, "Dict missing `resourceType`, is it a FHIR dict?"
        klass = FHIR_CLASSES.get_fhir_model_class(resource['resourceType'])
        _ = klass.parse_obj(resource)
        if validate:
            # trigger object traversal, see monkey patch below, at bottom of file
            _.dict()
        return ParseResult(resource=_, exception=None, path=None, resource_id=_.id)
    except (ValidationError, AssertionError) as e:
        return ParseResult(resource=None, exception=e, path=None, resource_id=resource.get('id', None))


def _is_ndjson(file_path: pathlib.Path) -> bool:
    """Open file, read all lines as json."""
    fp = _to_file(file_path)
    try:
        with fp:
            for line in fp.readlines():
                orjson.loads(line)
                break
        return True
    except Exception as e:  # noqa
        return False


def _to_file(file_path):
    """Open file appropriately."""
    if file_path.name.endswith('gz'):
        fp = io.TextIOWrapper(io.BufferedReader(gzip.GzipFile(file_path)))  # noqa
    else:
        fp = open(file_path, "rb")
    return fp


def _is_json_file(name: str) -> bool:
    """Files we are interested in"""
    if name.endswith('json.gz'):
        return True
    if name.endswith('json'):
        return True
    return False


def _has_entries(_: ParseResult):
    """"""
    if _.resource is None:
        return False
    return _.resource.resource_type in ["Bundle", "List"] and _.resource.entry is not None


def _entry_iterator(parse_result: ParseResult) -> Iterator[ParseResult]:
    """See if there are entries"""
    if not _has_entries(parse_result):
        yield parse_result
    else:
        _path = parse_result.path
        offset = 0
        if parse_result.resource.entry and len(parse_result.resource.entry) > 0:
            for _ in parse_result.resource.entry:
                if _ is None:
                    break
                if hasattr(_, 'resource'):  # BundleEntry
                    yield ParseResult(path=_path, resource=_.resource, offset=offset, exception=None)
                elif hasattr(_, 'item'):  # ListEntry
                    yield ParseResult(path=_path, resource=_.item, offset=offset, exception=None)
                else:
                    yield ParseResult(path=_path, resource=_.item, offset=offset, exception=None)
                offset += 1
    pass


def directory_reader(directory_path: pathlib.Path, pattern: str = '*.*', validate=True) -> Iterator[ParseResult]:
    """Extract FHIR resources from directory"""

    assert directory_path.is_dir(), f"{directory_path.name} is not a directory"

    input_files = [_ for _ in directory_path.glob(pattern) if _is_json_file(_.name)]
    for input_file in input_files:
        logger.info(input_file)
        if not input_file.is_file():
            continue
        is_ndjson = _is_ndjson(input_file)
        fp = _to_file(input_file)
        with fp:
            if is_ndjson:
                offset = 0
                for line in fp.readlines():
                    parse_result = parse_obj(orjson.loads(line), validate)
                    parse_result.path = input_file
                    parse_result.offset = offset
                    for _ in _entry_iterator(parse_result):
                        # print(_.offset, 'is_ndjson', input_file, parse_result.resource.id, parse_result.resource.resource_type)
                        yield _
                    offset += 1
            else:
                # look for json bundles
                _ = orjson.loads(fp.read())
                # not a bundle
                parse_result = parse_obj(_, validate)
                parse_result.path = input_file
                parse_result.offset = 0
                for _ in _entry_iterator(parse_result):
                    yield _
                continue
