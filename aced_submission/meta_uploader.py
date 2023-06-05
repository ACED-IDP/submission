import io
import logging
import pathlib
from collections import defaultdict
from datetime import datetime
from itertools import islice
from typing import List

import inflection
import yaml
from dictionaryutils import DataDictionary, dictionary
from yaml import SafeLoader

import psycopg2
import json

from aced_submission.pelican import DataDictionaryTraversal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)

LOGGED_ALREADY = []


def _connect_to_postgres():
    """Connect to postgres based on environmental variables."""
    return psycopg2.connect('')


def _init_dictionary(root_dir_=None, dictionary_url=None):
    """Initialize gen3 data dictionary from either directory or url"""
    d = DataDictionary(root_dir=root_dir_, url=dictionary_url)
    dictionary.init(d)
    # the gdcdatamodel expects dictionary initiated on load, so this can't be
    # imported on module level
    from gdcdatamodel import models as md

    return d, md


def _table_mappings(dictionary_path, dictionary_url):
    """Gen3 vertex/edge table mappings."""
    _dictionary, model = _init_dictionary(root_dir_=dictionary_path, dictionary_url=dictionary_url)
    ddt = DataDictionaryTraversal(model)
    desired_keys = [
        '__dst_class__',
        '__dst_src_assoc__',
        '__dst_table__',
        '__label__',
        '__src_class__',
        '__src_dst_assoc__',
        '__src_table__',
        '__tablename__'
    ]

    def _transform(ddt_) -> List[dict]:
        for d in ddt_.get_edges():
            yield {k.replace('_', ''): v for k, v in d.__dict__.items() if k in desired_keys}

    mapping = _transform(ddt)
    return mapping


def chunk(arr_range, arr_size):
    """Iterate in chunks."""
    arr_range = iter(arr_range)
    return iter(lambda: tuple(islice(arr_range, arr_size)), ())


def load_vertices(files, connection, dependency_order, project_id, mapping):
    """Load files into database vertices."""
    logger.info(f"Number of files available for load: {len(files)}")
    for entity_name in dependency_order:
        path = next(iter([fn for fn in files if str(fn).endswith(f"{entity_name}.ndjson")]), None)
        if not path:
            logger.warning(f"No file found for {entity_name} skipping")
            continue
        data_table_name = next(
            iter(
                set([m['dsttable'] for m in mapping if m['dstclass'].lower() == entity_name.lower()] +
                    [m['srctable'] for m in mapping if m['srcclass'].lower() == entity_name.lower()])
            ),
            None)
        if not data_table_name:
            logger.warning(f"No mapping found for {entity_name} skipping")
            continue
        logger.info(f"loading {path} into {data_table_name}")

        with connection.cursor() as cursor:
            with open(path) as f:
                # copy a block of records into a file like stringIO buffer
                record_count = 0
                for lines in chunk(f.readlines(), 1000):
                    buf = io.StringIO()
                    for line in lines:
                        record_count += 1
                        d_ = json.loads(line)
                        d_['object']['project_id'] = project_id
                        obj_str = json.dumps(d_['object'])
                        _csv = f"{d_['id']}\t{obj_str}\t{{}}\t{{}}\t{datetime.now()}".replace('\n', '\\n').replace("\\",
                                                                                                                   "\\\\")
                        _csv = _csv + '\n'
                        buf.write(_csv)
                    buf.seek(0)
                    # efficient way to write to postgres
                    cursor.copy_from(buf, data_table_name, sep='\t',
                                     columns=['node_id', '_props', 'acl', '_sysan', 'created'])
                    logger.info(f"wrote {record_count} records to {data_table_name} from {path}")
                    connection.commit()
        connection.commit()


def load_edges(files, connection, dependency_order, mapping, project_node_id):
    """Load files into database edges."""
    logger.info(f"Number of files available for load: {len(files)}")
    for entity_name in dependency_order:
        path = next(iter([fn for fn in files if str(fn).endswith(f"{entity_name}.ndjson")]), None)
        if not path:
            logger.warning(f"No file found for {entity_name} skipping")
            continue

        with connection.cursor() as cursor:
            print(path)
            with open(path) as f:
                # copy a block of records into a file like stringIO buffer
                record_count = 0
                for lines in chunk(f.readlines(), 100):
                    buffers = defaultdict(io.StringIO)
                    for line in lines:
                        d_ = json.loads(line)
                        relations = d_['relations']
                        if d_['name'] == 'ResearchStudy':
                            # link the ResearchStudy to the gen3 project
                            relations.append({"dst_id": project_node_id, "dst_name": "Project", "label": "project"})

                        if len(relations) == 0:
                            continue

                        record_count += 1
                        for relation in relations:

                            # entity_name_underscore = inflection.underscore(entity_name)
                            dst_name_camel = inflection.camelize(relation['dst_name'])

                            edge_table_mapping = next(
                                iter(
                                    [
                                        m for m in mapping
                                        if m['srcclass'] == entity_name and m['dstclass'] == relation['dst_name']
                                    ]
                                ),
                                None
                            )
                            if not edge_table_mapping and relation['dst_name'] in dependency_order:
                                msg = f"No mapping for src {entity_name} dst {relation['dst_name']}"
                                if msg not in LOGGED_ALREADY:
                                    logger.warning(msg)
                                    for m in mapping:
                                        logger.debug(m)
                                    LOGGED_ALREADY.append(msg)
                                continue
                            if not edge_table_mapping:
                                continue
                            table_name = edge_table_mapping['tablename']
                            # print(f"Mapping for src {entity_name} dst {relation['dst_name']} {table_name} {edge_table_mapping}")
                            buf = buffers[table_name]
                            # src_id | dst_id | acl | _sysan | _props | created |
                            buf.write(f"{d_['id']}|{relation['dst_id']}|{{}}|{{}}|{{}}|{datetime.now()}\n")
                    for table_name, buf in buffers.items():
                        buf.seek(0)
                        # efficient way to write to postgres
                        cursor.copy_from(buf, table_name, sep='|',
                                         columns=['src_id', 'dst_id', 'acl', '_sysan', '_props', 'created'])
                        logger.info(f"wrote {record_count} records to {table_name} from {path} {entity_name} {relation['dst_name']}")
        connection.commit()


def meta_upload(source_path, program, project, credentials_file, silent, dictionary_path, config_path, file_name_pattern='**/*.ndjson'):
    """Copy simplified json into Gen3."""
    assert pathlib.Path(source_path).is_dir(), f"{source_path} should be a directory"
    assert pathlib.Path(config_path).is_file(), f"{config_path} should be a file"

    config_path = pathlib.Path(config_path)
    assert config_path.is_file()
    with open(config_path) as fp:
        gen3_config = yaml.load(fp, SafeLoader)

    dependency_order = [c for c in gen3_config['dependency_order'] if not c.startswith('_')]
    dependency_order = [c for c in dependency_order if c not in ['Program', 'Project']]

    # check db connection
    conn = _connect_to_postgres()
    assert conn
    logger.info("Connected to postgres")

    # check program/project exist
    cur = conn.cursor()
    cur.execute("select node_id, _props from \"node_program\";")
    programs = cur.fetchall()
    programs = [{'node_id': p[0], '_props': p[1]} for p in programs]
    program = next(iter([p for p in programs if p['_props']['name'] == program]), None)
    assert program, f"{program} not found in node_program table"
    cur.execute("select node_id, _props from \"node_project\";")
    projects = cur.fetchall()
    projects = [{'node_id': p[0], '_props': p[1]} for p in projects]
    project_code = project
    project_node_id = next(iter([p['node_id'] for p in projects if p['_props']['code'] == project_code]), None)
    assert project_node_id, f"{project} not found in node_project"
    project_id = f"{program}-{project}"
    logger.info(f"Program and project exist: {project_id} {project_node_id}")

    # check files
    input_path = pathlib.Path(source_path)
    assert input_path.is_dir(), f"{input_path} should be a directory"
    files = [fn for fn in input_path.glob(file_name_pattern)]
    assert len(files) > 0, f"No files found at {input_path}/{file_name_pattern}"

    # check the mappings
    dictionary_path = dictionary_path if 'http' not in dictionary_path else None
    dictionary_url = dictionary_path if 'http' in dictionary_path else None
    mappings = [mapping for mapping in _table_mappings(dictionary_path, dictionary_url)]

    # load the files
    logger.info("Loading vertices")
    load_vertices(files, conn, dependency_order, project_id, mappings)

    logger.info("Loading edges")
    load_edges(files, conn, dependency_order, mappings, project_node_id)
    logger.info("Done")
