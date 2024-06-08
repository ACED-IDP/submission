import io
import logging
import pathlib
import uuid
from collections import defaultdict
from datetime import datetime
from itertools import islice
from typing import List

import yaml
from dictionaryutils import DataDictionary, dictionary
from yaml import SafeLoader

import psycopg2
import json
import inflection

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

        columns = ['node_id', '_props', 'acl', '_sysan', 'created']
        # Select only columns to be updated (in my case, all non-id columns)
        # update_set = ", ".join([f"{v}=EXCLUDED.{v}" for v in ['_props', 'acl', '_sysan', 'created']])
        update_set = ' _props=EXCLUDED._props, acl=EXCLUDED.acl, _sysan=EXCLUDED._sysan, created=EXCLUDED.created '

        with connection.cursor() as cursor:
            with open(path) as f:
                # copy a block of records into a file like stringIO buffer
                record_count = 0
                for lines in chunk(f.readlines(), 1000):
                    # Creates temporary empty table with same columns and types as
                    # the final table
                    cursor.execute(
                        f"""
                        CREATE TEMPORARY TABLE tmp_{data_table_name} (LIKE {data_table_name})
                        ON COMMIT DROP
                        """
                    )
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
                    cursor.copy_from(buf, f'tmp_{data_table_name}', sep='\t',
                                     columns=columns)
                    # handle conflicts
                    cursor.execute(
                        f"""
                        INSERT INTO {data_table_name}({', '.join(columns)})
                        SELECT  node_id, _props::jsonb, acl, _sysan, created FROM tmp_{data_table_name}
                        ON CONFLICT (node_id) DO UPDATE SET {update_set}
                        """
                    )
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

                        # TODO - ensure only one id per type simplifier
                        set_ = dict((v['dst_id'], v) for v in relations).values()
                        relations = [_ for _ in set_]

                        if d_['name'] in ['ResearchStudy', 'research_study']:
                            # link the ResearchStudy to the gen3 project
                            relations.append({"dst_id": project_node_id, "dst_name": "Project", "label": "project"})
                            logger.info(
                                f"adding project relation from project({project_node_id}) to research_study{d_['id']}")

                        if len(relations) == 0:
                            msg = f"No relations for {d_['name']}"
                            if msg not in LOGGED_ALREADY:
                                LOGGED_ALREADY.append(msg)
                                print(msg)
                            continue

                        record_count += 1
                        for relation in relations:

                            # entity_name_underscore = inflection.underscore(entity_name)
                            dst_name_camel = inflection.camelize(relation['dst_name'])

                            edge_table_mapping = next(
                                iter(
                                    [
                                        m for m in mapping
                                        if m['srcclass'] == entity_name and m['dstclass'] == dst_name_camel
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
                                msg = f"No mapping for src {entity_name} dst {relation['dst_name']}"
                                if msg not in LOGGED_ALREADY:
                                    print(msg)
                                    LOGGED_ALREADY.append(msg)
                                continue
                            table_name = edge_table_mapping['tablename']
                            # print(f"Mapping for src {entity_name} dst {relation['dst_name']} {table_name} {edge_table_mapping}")
                            buf = buffers[table_name]
                            # src_id | dst_id | acl | _sysan | _props | created |
                            buf.write(f"{d_['id']}|{relation['dst_id']}|{{}}|{{}}|{{}}|{datetime.now()}\n")
                    for table_name, buf in buffers.items():
                        buf.seek(0)
                        # Creates temporary empty table with same columns and types as
                        # the final table
                        cursor.execute(
                            f"""
                            CREATE TEMPORARY TABLE "tmp_{table_name}" (LIKE "{table_name}")
                            ON COMMIT DROP
                            """
                        )

                        columns = ['src_id', 'dst_id', 'acl', '_sysan', '_props', 'created']
                        update_set = ", ".join([f"{v}=EXCLUDED.{v}" for v in ['acl', '_sysan', '_props', 'created']])
                        # efficient way to write to postgres
                        cursor.copy_from(buf, f"tmp_{table_name}", sep='|',
                                         columns=columns)
                        # handle conflicts
                        cursor.execute(
                            f"""
                            INSERT INTO "{table_name}" ({', '.join(columns)})
                            SELECT  {', '.join(columns)} FROM "tmp_{table_name}"
                            ON CONFLICT (src_id, dst_id) DO UPDATE SET {update_set}
                            """
                        )
                        logger.info(
                            f"wrote {record_count} records to {table_name} from {path} {entity_name} {relation['dst_name']}")
                        connection.commit()
        connection.commit()


def meta_upload(source_path, program, project, silent, dictionary_path, config_path,
                file_name_pattern='**/*.ndjson'):
    """Copy simplified json into Gen3."""
    assert pathlib.Path(source_path).is_dir(), f"{source_path} should be a directory"
    assert pathlib.Path(config_path).is_file(), f"{config_path} should be a file"
    assert dictionary_path, "dictionary_path cannot be empty"

    config_path = pathlib.Path(config_path)
    assert config_path.is_file()
    with open(config_path) as fp:
        gen3_config = yaml.load(fp, SafeLoader)

    dependency_order = [c for c in gen3_config['dependency_order'] if not c.startswith('_')]
    dependency_order = [c for c in dependency_order if c not in ['Program', 'Project']]

    ensure_project(program, project)

    # check db connection
    conn = _connect_to_postgres()
    assert conn
    logger.info("Connected to postgres")

    # check program/project exist
    cur = conn.cursor()
    cur.execute("select node_id, _props from \"node_program\";")
    programs = cur.fetchall()
    programs = [{'node_id': p[0], '_props': p[1]} for p in programs]
    _ = next(iter([p for p in programs if p['_props']['name'] == program]), None)
    assert _, f"{program} not found in node_program table"
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
    dictionary_dir = dictionary_path if 'http' not in dictionary_path else None
    dictionary_url = dictionary_path if 'http' in dictionary_path else None
    mappings = [mapping for mapping in _table_mappings(dictionary_dir, dictionary_url)]

    # load the files
    logger.info("Loading vertices")
    load_vertices(files, conn, dependency_order, project_id, mappings)

    logger.info("Loading edges")
    load_edges(files, conn, dependency_order, mappings, project_node_id)
    logger.info("Done")


# see https://github.com/uc-cdis/sheepdog/blob/master/sheepdog/globals.py#L51-L52
PROGRAM_SEED = uuid.UUID("85b08c6a-56a6-4474-9c30-b65abfd214a8")
PROJECT_SEED = uuid.UUID("249b4405-2c69-45d9-96bc-7410333d5d80")


def ensure_project(program, project) -> bool:
    """Ensure project exists in sheepdog database."""
    # check db connection
    conn = _connect_to_postgres()
    assert conn
    logger.info("Connected to postgres")

    # check program/project exist
    cur = conn.cursor()
    cur.execute("select node_id, _props from \"node_program\";")
    programs = cur.fetchall()
    programs = [{'node_id': p[0], '_props': p[1]} for p in programs]
    _ = next(iter([p for p in programs if p['_props']['name'] == program]), None)
    program_node_id = None
    if not _:  # program does not exist
        logger.info(f"Program {program} does not exist")
        program_node_id = str(uuid.uuid5(PROGRAM_SEED, program))
        cur.execute(
            "INSERT INTO node_program(node_id, _props) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (program_node_id, json.dumps({'name': program, 'type': 'program', "dbgap_accession_number": program}))
        )
        conn.commit()
        logger.info(f"Created Program {program}: {program_node_id}")
    else:
        program_node_id = _['node_id']
        logger.info(f"Program {program} exists: {program_node_id}")

    cur.execute("""
        select node_id, _props->>'code' as code  from node_project where node_id in (select src_id
        from
        edge_projectmemberofprogram
        where dst_id = (select node_id from node_program where _props->>'name' = %s)) and _props->>'code' = %s ;""",
        (program, project,)
    )
    project_node_id = None
    _ = cur.fetchone()
    if _:
        project_node_id, _ = _

    if not project_node_id:  # project does not exist
        logger.info(f"Project {project} does not exist")
        project_node_id = str(uuid.uuid5(PROJECT_SEED, project))
        cur.execute(
            "INSERT INTO node_project(node_id, _props) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (project_node_id,
             json.dumps({'code': project, 'type': 'project', "state": "open", "dbgap_accession_number": project}))
        )
        conn.commit()
        logger.info(f"Created Project {project}: {project_node_id}")
        cur.execute(
            "INSERT INTO edge_projectmemberofprogram(src_id, dst_id)  VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (project_node_id, program_node_id)
        )
        conn.commit()
        logger.info(f"Created edge_projectmemberofprogram between {project_node_id} -> {program_node_id}")

    project_id = f"{program}-{project}"
    logger.info(f"Program and project exist: {project_id} {project_node_id}")


def empty_project(config_path, dictionary_path, program, project):
    """Remove all nodes from metadata graph."""

    config_path = pathlib.Path(config_path)
    assert config_path.is_file()
    with open(config_path) as fp:
        gen3_config = yaml.load(fp, SafeLoader)

    project_id = f"{program}-{project}"
    logger.info(f"Emptying project {project_id}")
    dependency_order = [c for c in gen3_config['dependency_order'] if not c.startswith('_')]
    dependency_order = [c for c in dependency_order if c not in ['Program', 'Project']]

    dictionary_dir = dictionary_path if 'http' not in dictionary_path else None
    dictionary_url = dictionary_path if 'http' in dictionary_path else None
    mappings = [mapping for mapping in _table_mappings(dictionary_dir, dictionary_url)]

    conn = _connect_to_postgres()
    for entity_name in dependency_order:
        data_table_name = next(
            iter(
                set([m['dsttable'] for m in mappings if m['dstclass'].lower() == entity_name.lower()] +
                    [m['srctable'] for m in mappings if m['srcclass'].lower() == entity_name.lower()])
            ),
            None)
        if not data_table_name:
            logger.warning(f"No mapping found for {entity_name} skipping")
            continue
        logger.info(f"Truncating {data_table_name} for {project_id}")
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {data_table_name} WHERE _props->>'project_id' = %s", (project_id,))
            conn.commit()

    conn.commit()
    logger.info(f"Done emptying project {project_id}")
