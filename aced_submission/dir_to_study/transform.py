#!/usr/bin/env python3
import unicodedata
import uuid
from datetime import timezone, datetime
import hashlib

import magic
import pathlib
import click

import orjson
import mimetypes

from aced_submission import EmitterContextManager

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


def create_research_study(name, description):
    """Creates bare-bones study."""
    study = {
        'title': name,
        'id': str(uuid.uuid5(ACED_NAMESPACE, name)),
        'description': description,
        'status': 'active',
        "resourceType": "ResearchStudy",
    }
    return study


def md5sum(file_name):
    """Calculate the hash and size."""
    md5_hash = hashlib.md5()
    file_name = unicodedata.normalize("NFKD", str(file_name))
    with open(file_name, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)

    return md5_hash.hexdigest()


def _extract_fhir_resources(file, input_path):
    """TODO A placeholder to parse other resources from file."""
    pass


@click.command('from_dir')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='Gen3 program-project'
              )
@click.option('--input_path', required=True,
              default=None,
              show_default=True,
              help='Read files from this path'
              )
@click.option('--remove_path_prefix', required=True,
              default="/",
              show_default=True,
              help='Remove prefix from file paths.  '
                   'Creates well-known form for achieving reproducible directories independent '
                   'of the directory the files were collected from.'
              )
@click.option('--output_path', required=True,
              default=None,
              show_default=True,
              help='Write FHIR resources to this path'
              )
@click.option('--pattern',
              default='**/*',
              show_default=True,
              help='File names to match.')
def cli(project_id, input_path, remove_path_prefix, output_path, pattern):
    """Create minimal study meta from matching files in input path."""
    dir_to_study(project_id, input_path, remove_path_prefix, output_path, pattern)


def dir_to_study(project_id, input_path, remove_path_prefix, output_path, pattern):
    """Transform ResearchStudy, DocumentReference from matching files in input path."""
    # print(project_id, path, pattern)
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)

    for _ in [input_path]:
        assert _.exists(), f"input_path {_} does not exist."
        assert _.is_dir(), f"input_path {_} is not a directory."

    for _ in [output_path]:
        if not _.exists():
            print(f"output_path {_} does not exist, creating...")
            _.mkdir(parents=True, exist_ok=True)

    _magic = magic.Magic(mime=True, uncompress=True)
    program, project = project_id.split('-')
    research_study = create_research_study(project, f"A study with files from {input_path}/{pattern}")

    with EmitterContextManager(output_path, file_mode="wb") as emitter:

        emitter.emit('ResearchStudy').write(
            orjson.dumps(research_study, orjson.OPT_APPEND_NEWLINE))

        for file in input_path.glob(pattern):
            if file.is_dir():
                continue
            stat = file.stat()
            modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            mime, encoding = mimetypes.guess_type(file)
            if not mime:
                mime = _magic.from_file(file)

            _extract_fhir_resources(file, input_path)

            document_reference = {
              "resourceType": "DocumentReference",
              "status": "current",
              "docStatus": "final",
              "id": str(uuid.uuid5(ACED_NAMESPACE, research_study['id'] + f"::{file}")),
              "date": modified.isoformat(),  # When this document reference was created
              "content": [{
                "attachment": {
                    "extension": [{
                        "url": "http://aced-idp.org/fhir/StructureDefinition/md5",
                        "valueString": md5sum(file)
                    }, {
                        "url": "http://aced-idp.org/fhir/StructureDefinition/source_path",
                        "valueUrl": f"file:///{file}"
                    }],
                    "contentType": mime,  # Mime type of the content, with charset etc.
                    "url": f"file:///{str(file).replace(remove_path_prefix, '', 1)}",  # Uri where the data can be found
                    "size": stat.st_size,  # Number of bytes of content (if url provided)
                    "title": file.name,  # Label to display in place of the data
                    "creation": modified.isoformat()  # Date attachment was first created
                },
              }],
              "subject": {
                  "reference": f"ResearchStudy/{research_study['id']}"  # Who/what is the subject of the document
              }
            }
            emitter.emit('DocumentReference').write(orjson.dumps(document_reference, option=orjson.OPT_APPEND_NEWLINE))


if __name__ == '__main__':
    cli()
