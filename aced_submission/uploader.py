#!/usr/bin/env python3
import datetime
import json
import logging
import pathlib
import sys
import urllib
import uuid
from dataclasses import dataclass
from itertools import islice
from multiprocessing.pool import Pool
from time import sleep
from typing import Iterator

import click
import jwt
import requests
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3.index import Gen3Index
from orjson import orjson
from tqdm import tqdm

from aced_submission import NaturalOrderGroup

logger = logging.getLogger(__name__)

LOGGED_ALREADY = set({})

ACED_CODEABLE_CONCEPT = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced.ipd/CodeableConcept')
ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


@dataclass
class UploadResult:
    """Results of Upload DocumentReference."""
    document_reference: dict
    """The source document reference."""
    elapsed: datetime.timedelta
    """Amount of time it took to upload."""
    exception: Exception = None
    """On error."""


def _chunk(arr_range, arr_size):
    """Iterate in chunks."""
    arr_range = iter(arr_range)
    return iter(lambda: tuple(islice(arr_range, arr_size)), ())


@click.group(cls=NaturalOrderGroup)
def files():
    """Project files (Omics, Imaging, ...)."""
    pass


def extract_endpoint(gen3_credentials_file):
    """Get base url of jwt issuer claim."""
    with open(gen3_credentials_file) as fp:
        api_key = json.load(fp)['api_key']
        claims = jwt.decode(api_key, options={"verify_signature": False})
        assert 'iss' in claims
        return claims['iss'].replace('/user', '')


def _upload_document_reference(document_reference: dict, bucket_name: str,
                               program: str, project: str, duplicate_check: bool, credentials_file: str,
                               source_path: str) -> UploadResult:
    """Write a single document reference to indexd and upload file."""

    try:
        start = datetime.datetime.now()
        # print(('starting', document_reference['id'], start.isoformat()))

        file_client, index_client = _gen3_services(credentials_file)

        attachment, md5sum, source_path_extension = _extract_extensions(document_reference)

        source_path = _extract_source_path(attachment, source_path, source_path_extension)

        file_name = source_path.lstrip('./').lstrip('file:///')
        object_name = attachment['url'].lstrip('./').lstrip('file:///')

        metadata = _update_indexd(attachment, bucket_name, document_reference, duplicate_check, index_client, md5sum,
                                  object_name, program, project)

        # create a record in gen3 using document_reference's id as guid, get a signed url
        # SYNC

        document = file_client.upload_file_to_guid(guid=document_reference['id'], file_name=object_name, bucket=bucket_name)
        assert 'url' in document, document
        signed_url = urllib.parse.unquote(document['url'])
        file_name = pathlib.Path(file_name)
        assert file_name.exists(), f"{file_name} does not exist"

        _upload_file_to_signed_url(file_name, md5sum, metadata, signed_url)

        end = datetime.datetime.now()
        # print(('complete', document_reference['id'], end.isoformat(), end-start, attachment["size"]))

        return UploadResult(document_reference, end - start)
    except Exception as e:  # noqa
        return UploadResult(document_reference, None, e)


def _read_in_chunks(file_object, chunk_size):
    """Iterator to read file in chunks"""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def _upload_file_to_signed_url(file_name, md5sum, metadata, signed_url):
    """Upload file """

    # When you use this header, Amazon S3 checks the object against the provided MD5 value and,
    # if they do not match, returns an error.

    # content_md5 = base64.b64encode(bytes.fromhex(md5sum))
    # headers = {'Content-MD5': content_md5}
    # # attach our metadata to s3 object
    # for key, value in metadata.items():
    #     headers[f"x-amz-meta-{key}"] = value

    with open(file_name, 'rb') as fp:
        # SYNC
        response = requests.put(signed_url, data=fp)
        response_text = response.text
        assert response.status_code == 200, (signed_url, response_text)


def _update_indexd(attachment, bucket_name, document_reference, duplicate_check, index_client, md5sum, object_name,
                   program, project):
    hashes = {'md5': md5sum}
    assert 'id' in document_reference, document_reference
    guid = document_reference['id']
    metadata = {
        **{
            'datanode_type': 'DocumentReference',
            'datanode_object_id': guid
        },
        **hashes}
    # SYNC
    existing_record = None
    s3_url = f"s3://{bucket_name}/{guid}/{object_name}"
    if duplicate_check:
        try:
            existing_record = index_client.get_record(guid=document_reference["id"])
        except Exception: # noqa
            pass
        if existing_record:
            skip_delete = all([
                existing_record['hashes']['md5'] == md5sum,
                s3_url in existing_record['urls']
            ])
            if not skip_delete:
                # SYNC
                logger.debug(f"Deleting existing record {document_reference['id']}")
                index_client.delete_record(guid=document_reference["id"])
                existing_record = None
    if not existing_record:
        try:
            _ = index_client.create_record(
                did=document_reference["id"],
                hashes=hashes,
                size=attachment["size"],
                authz=[f'/programs/{program}/projects/{project}'],
                file_name=object_name,
                metadata=metadata,
                urls=[s3_url]  # TODO make a DRS URL
            )
        except (requests.exceptions.HTTPError, AssertionError) as e:
            if not ('already exists' in str(e)):
                raise e
            logger.info(f"indexd record already exists, continuing upload. {document_reference['id']}")
    return metadata


def _extract_source_path(attachment, source_path, source_path_extension) -> str:
    if source_path:
        source_path = pathlib.Path(source_path)
        assert source_path.is_dir(), f"Path is not a directory {source_path}"
        source_path = source_path / attachment['url'].lstrip('./').lstrip('file:///')
        source_path = str(source_path)
    else:
        if len(source_path_extension) == 1:  # "Missing source_path extension."
            source_path = source_path_extension[0]['valueUrl']
        else:
            source_path = attachment['url'].lstrip('./').lstrip('file:///')
    return source_path


def _extract_extensions(document_reference):
    """Extract useful data from document_reference."""
    attachment = document_reference['content'][0]['attachment']
    md5_extension = [_ for _ in attachment['extension'] if
                     _['url'] == "http://aced-idp.org/fhir/StructureDefinition/md5"]
    assert len(md5_extension) == 1, "Missing MD5 extension."
    md5sum = md5_extension[0]['valueString']
    source_path_extension = [_ for _ in attachment['extension'] if
                             _['url'] == "http://aced-idp.org/fhir/StructureDefinition/source_path"]
    return attachment, md5sum, source_path_extension


def _gen3_services(credentials_file: str) -> (Gen3File, Gen3Index):
    """Create Gen3 Services."""
    credentials_file = str(pathlib.Path(credentials_file).expanduser())
    endpoint = extract_endpoint(credentials_file)
    # logger.debug(endpoint)
    auth = Gen3Auth(endpoint, refresh_file=credentials_file)
    file_client = Gen3File(endpoint, auth)
    index_client = Gen3Index(endpoint, auth)
    return file_client, index_client


def document_reference_reader(document_reference_path) -> Iterator[dict]:
    """Read DocumentReference.ndjson file or bundle."""
    if 'ndjson' in document_reference_path:
        with open(document_reference_path) as fp:
            for _ in fp.readlines():
                yield orjson.loads(_)
    else:
        document_reference_path = pathlib.Path(document_reference_path)
        for input_file in document_reference_path.glob('*.json'):
            with open(input_file, "rb") as fp:
                bundle_ = orjson.loads(fp.read())
                if 'entry' not in bundle_:
                    print(f"No 'entry' in bundle {input_file} ")
                    break
            for entry in bundle_['entry']:
                resource = entry['resource']
                if resource['resourceType'] == 'DocumentReference':
                    yield resource


@files.command(name='upload')
@click.option('--bucket_name', show_default=True,
              help='Destination bucket name')
@click.option('--document_reference_path', required=True, default=None, show_default=True,
              help='Path to DocumentReference.ndjson')
@click.option('--source_path', required=False, default=None, show_default=True,
              help='Path on local file system')
@click.option('--program', required=True, show_default=True,
              help='Gen3 program')
@click.option('--project', required=True, show_default=True,
              help='Gen3 project')
@click.option('--credentials_file', default='~/.gen3/credentials.json', show_default=True,
              help='API credentials file downloaded from gen3 profile.')
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True,
              help="Check for existing indexd records")
@click.option('--worker_count', default=10, show_default=True,
              help="Number of worker processes")
@click.option('--silent', default=False, is_flag=True, show_default=True,
              help="No progress bar, or other output")
@click.option('--state_dir', default='~/.gen3/aced-uploader', show_default=True,
              help='Directory for upload status')
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
def upload_document_reference(bucket_name, document_reference_path, source_path, program, project, credentials_file,
                              duplicate_check, worker_count, silent, state_dir, ignore_state):
    """Upload data file associated with DocumentReference.

    """
    _ = pathlib.Path(document_reference_path)
    assert _.is_file() or _.is_dir(), f"{document_reference_path} directory does not exist"

    state_dir = pathlib.Path(state_dir).expanduser()
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / "state.ndjson"

    # all attempts ids are incomplete until they succeed
    incomplete = set()
    # key:document_reference.id of completed file transfers
    completed = set()
    # key:document_reference.id  of failed file transfers
    exceptions = {}

    # for unhandled exceptions: map multiprocessing tasks results to document_reference
    # keyed by result
    document_reference_lookup = {}

    # progress bar control
    document_references_size = 0
    # loop control
    document_references_length = 0

    already_uploaded = set()
    if not ignore_state and state_file.exists():
        with open(state_file, "rb") as fp:
            for _ in fp.readlines():
                state = orjson.loads(_)
                already_uploaded.update([_ for _ in state['completed']])

    for _ in document_reference_reader(document_reference_path):
        if _['id'] not in already_uploaded:
            incomplete.add(_['id'])
            document_references_size += _['content'][0]['attachment']['size']
            document_references_length += 1
        else:
            if not silent:
                print(f"{_['id']} already uploaded, skipping", file=sys.stderr)

    # re-open file, process it a chunk at a time
    with Pool(processes=worker_count) as pool:
        results = []
        for document_reference in document_reference_reader(document_reference_path):
            if document_reference['id'] in already_uploaded:
                continue
            result = pool.apply_async(
                func=_upload_document_reference,
                args=(
                    document_reference,
                    bucket_name,
                    program,
                    project,
                    duplicate_check,
                    credentials_file,
                    source_path
                )
            )
            results.append(result)
            document_reference_lookup[id(result)] = document_reference['id']

        # close the process pool
        pool.close()

        # poll the results every sec.
        with tqdm(total=document_references_size, unit='B', disable=silent,
                  unit_scale=True, unit_divisor=1024) as pbar:
            while True:
                results_to_remove = []
                for record in results:
                    if record.ready() and record.successful():
                        r = record.get()
                        # print(f'ready and successful {id(r)}')
                        if r.exception:
                            exceptions[r.document_reference['id']] = {
                                    'exception': str(r.exception),
                                    'document_reference': {
                                        'id': r.document_reference
                                    }
                                }
                        elif r.document_reference['id'] not in completed:
                            completed.add(r.document_reference['id'])
                            incomplete.remove(r.document_reference['id'])

                        results_to_remove.append(record)
                        document_references_length = document_references_length - 1
                        pbar.set_postfix(file=f"{r.document_reference['id'][-6:]}", elapsed=f"{r.elapsed}")
                        pbar.update(r.document_reference['content'][0]['attachment']['size'])
                        sleep(.1)  # give screen a chance to refresh

                    if record.ready() and not record.successful():
                        print('record.ready() and not record.successful()')
                        # capture exception, we shouldn't get here as all exception should be caught
                        document_reference_id = document_reference_lookup[id(record)]
                        try:
                            record.get()
                        except Exception as e:  # noqa
                            if document_reference_id not in exceptions:
                                exceptions[document_reference_id] = {
                                    'exception': str(e),
                                    'document_reference': {
                                        'id': document_reference_id
                                    }
                                }
                                document_references_length = document_references_length - 1
                                # print('not successful', document_references_length, e)

                if document_references_length == 0:
                    break

                # print(f'sleeping(1) document_references_length: {document_references_length}')
                sleep(1)

                # using list comprehension to cull processed results
                results = [_ for _ in results if _ not in results_to_remove]

        with open(state_file, "a+b") as fp:
            fp.write(orjson.dumps(
                    {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'completed': [_ for _ in completed],
                        'incomplete': [_ for _ in incomplete],
                        'exceptions': exceptions
                    },
                    option=orjson.OPT_APPEND_NEWLINE
                ))

        if not silent:
            print(f"Wrote state to {state_file}", file=sys.stderr)
        if len(incomplete) == 0:
            if not silent:
                print('OK', file=sys.stderr)
            exit(0)
        else:
            if not silent:
                print('Incomplete transfers:', [_ for _ in incomplete], file=sys.stderr)
                print('Errors:', [f"{_}: {exceptions[_]['exception']}" for _ in exceptions], file=sys.stderr)
            exit(1)


if __name__ == '__main__':
    files()
