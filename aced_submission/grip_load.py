import os
import orjson
import requests
from typing import List, Generator


NGINX_PATH = "writer"
PROTOBUF_PATH = "v1/graph"


def bulk_load(grip_service: str, graph_name: str, project_id: str, directory_path: str, output: dict, access_token: str) -> List[dict]:
    """Loads a directory of .ndjson or .gz files to grip.
        Args:
                grip_service: The name of the k8s service that refers to the internal cluster-ip for the running grip deployment
                graph_name: The name of the graph
                project_id: the Gen3 program-project to be used
                directory_path: the file path to the directory that contains the FHIR files
                output: the dict the holds output logs
                access_token: JWT token that contains user identification information for permissions checking

        TODO: implement FHIR schema in grip
                    so that edges that don't validate in graph are rejected
    """
    response_json = []

    # List graphs and check to see if graph name is amoung the graphs listed
    output["logs"].append(f"loading files into {graph_name} from {directory_path}")

    assert os.path.isdir(directory_path), output["logs"].append(f"directory path {directory_path} is not a directory")
    importable_files = [f for f in os.listdir(directory_path) if any([f.endswith(".json"), f.endswith(".gz"), f.endswith(".ndjson")])]
    assert len(importable_files) > 0, output["logs"].append("No .json, .gz or .ndjson files have been uploaded")

    output["logs"].append(f"files in {directory_path}: {str(os.listdir(directory_path))}")
    output["logs"].append(f"importable files found: {str(importable_files)}")

    for file in importable_files:
        file_path = f"{directory_path}/{file}"
        output["logs"].append(f"loading file: {file_path}")
        graph_component = "edge" if "edge" in file_path else "vertex"
        with open(file_path, 'rb') as file_io:
            files = {'file': (file_path, file_io)}
            response = requests.post(
                f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/bulk-load/{project_id}",
                data={"types": graph_component},
                headers={"Authorization": f"bearer {access_token}"},
                files=files
            )

        json_data = response.json()
        response_json.append(json_data)
        output["logs"].append(f"json data: {json_data}")

    return response_json


def bulk_load_raw(grip_service: str, graph_name: str, project_id: str, ndjson_file_path: str, output: dict, access_token: str) -> dict:
    output["logs"].append(f"loading files into {graph_name} from {ndjson_file_path}")
    assert os.path.isfile(ndjson_file_path), output["logs"].append(f"file path {ndjson_file_path} is not a file")

    with open(ndjson_file_path, 'r') as file_io:
        files = {'file': (ndjson_file_path, file_io)}
        response = requests.post(
            f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/bulk-load-raw/{project_id}",
            headers={"Authorization": f"bearer {access_token}"},
            files=files
        )

    json_data = response.json()
    output["logs"].append(json_data)
    return json_data


def bulk_delete(grip_service: str, graph_name: str, vertices: List[str], project_id: str,  edges: List[str], output: dict, access_token: str) -> dict:
    """Deletes graph elements from a grip graph.
        Args:
            graph_name: The name of the graph
            project_id: the Gen3 program-project to be used
            vertices:   A list of vertex ids
            edges:      A list of edge ids
    """
    data = {"graph": graph_name,
            "vertices": vertices,
            "edges": edges
            }
    response = requests.delete(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/bulk-delete/{project_id}",
                               data=orjson.dumps(data),
                               headers={"Authorization": f"bearer {access_token}"}
                               )

    json_data = response.json()
    output["logs"].append(f"bulk-delete response: {json_data}")

    return json_data


def delete_edge(grip_service: str, graph_name: str, edge_id: str, project_id: str, output: dict, access_token: str) -> dict:
    """Deletes one edge from the specified graph"""
    response = requests.delete(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/del-edge/{project_id}/{edge_id}",
                               headers={"Authorization": f"bearer {access_token}"}
                               )

    json_data = response.json()
    output["logs"].append(f"del-edge response: {json_data}")

    return json_data


def delete_vertex(grip_service: str, graph_name: str, vertex_id: str, project_id: str, output: dict, access_token: str) -> dict:
    """Deletes one vertex from the specified graph"""
    response = requests.delete(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/del-vertex/{project_id}/{vertex_id}",
                               headers={"Authorization": f"bearer {access_token}"}
                               )

    json_data = response.json()
    output["logs"].append(f"del-vertex response: {json_data}")

    return json_data


def add_vertex(grip_service: str, graph_name: str, vertex: dict, project_id: str, output: dict, access_token: str) -> dict:
    """Adds one vertex to the specified graph
        required vertex format:
            {
                "gid": str, id of vertex,
                "label": str, resource type,
                "data": dict, vertex properties.
            }
    """
    response = requests.post(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/add-vertex/{project_id}/{vertex['gid']}",
                             headers={"Authorization": f"bearer {access_token}"},
                             json=vertex
                             )

    json_data = response.json()
    output["logs"].append(f"add-vertex response: {json_data}")

    return json_data


def add_edge(grip_service: str, graph_name: str, edge: dict, project_id: str, output: dict, access_token: str) -> dict:
    """Adds one edge to the specified graph
        required edge format:
            {
                "gid": str, id of edge,
                "label": str, rel,
                "from": str, backref vertex Id,
                "to": str, to vertex ID,
                "data": dict, optional edge properties.
            }
    """
    response = requests.post(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/add-edge/{project_id}/{edge['gid']}",
                             headers={"Authorization": f"bearer {access_token}"},
                             json=edge
                             )

    json_data = response.json()
    output["logs"].append(f"add-edge response: {json_data}")

    return json_data


def list_graphs(grip_service: str, output: dict, access_token: str) -> dict:
    """Returns a list of all graph names in grip"""
    response = requests.get(f"http://{grip_service}:8201/{NGINX_PATH}/list-graphs",
                            headers={"Authorization": f"bearer {access_token}"}
                            )

    json_data = response.json()
    output["logs"].append(f"list-graphs response: {json_data}")

    assert "data" in json_data and "graphs" in json_data["data"], output["logs"].append("Expecting json_data['data']['graphs'] to exist")
    return json_data


def add_schema(grip_service: str, graph_name: str, schema_path: str, project_id: str, output: dict, access_token: str, json_schema: bool) -> dict:
    """Adds a schema to a graph in grip. If json_schema is specified then a json_schema file should be used for schema_path.
            if json_schema flag is not specified then normal grip schema format should be used."""

    assert os.path.isfile(schema_path), output["logs"].append(f"{schema_path} is not a file")
    with open(schema_path, 'rb') as file_io:
        files = {'file': (schema_path, file_io)}
        path = f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/add-schema/{project_id}"
        if json_schema:
            path = f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/add-json-schema/{project_id}"
        response = requests.post(
            path,
            headers={"Authorization": f"bearer {access_token}"},
            files=files
        )

    json_data = response.json()
    output["logs"].append(f"add-schema response: {json_data}")
    return json_data


def add_graph(grip_service: str, graph_name: str, project_id: str, output: dict, access_token: str) -> dict:
    """Creates a new graph"""

    response = requests.post(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/add-graph/{project_id}",
                             headers={"Authorization": f"bearer {access_token}"})

    json_data = response.json()
    output["logs"].append(f"add-graph response: {json_data}")
    return json_data


def drop_graph(grip_service: str, graph_name: str, project_id: str, output: dict, access_token: str) -> dict:
    """Deletes a graph and all of its data"""

    exists = graph_exists(grip_service, graph_name, output, access_token)
    # Not going to get a grip error if you attempt to delete something that doesn't exist it grip,
    # But it might be good to still have an assert statement here to catch the fact that the graph doesn't exist
    assert exists, output["logs"].append(f"Graph {graph_name} does not exist in grip")
    response = requests.delete(f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/del-graph/{project_id}",
                               headers={"Authorization": f"bearer {access_token}"})

    json_data = response.json()
    output["logs"].append(f"del-graph response: {json_data}")
    return json_data


def graph_exists(grip_service: str, graph_name: str, output: dict, access_token: str) -> bool:
    """Check to see if the provided graph name exists in grip"""
    existing_graphs = list_graphs(grip_service, output, access_token)["data"]["graphs"]
    return graph_name in existing_graphs


def delete_project(grip_service: str, graph_name: str, project_id: str, output: dict, access_token: str) -> dict:
    """Delete a gen3 project entirely from a grip graph"""
    response = requests.delete(
            f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/proj-delete/{project_id}",
            headers={"Authorization": f"bearer {access_token}"},
            )

    json_data = response.json()
    output["logs"].append(f"proj-delete response: {json_data}")
    return json_data


def get_project_data(grip_service: str, graph_name: str, project_id: str, output: dict, access_token: str) -> Generator[dict, None, None]:
    """Retrieves all of the data for a given project id on a given graph"""
    response = requests.get(
        f"http://{grip_service}:8201/{NGINX_PATH}/{graph_name}/get-vertices/{project_id}",
        headers={"Authorization": f"bearer {access_token}"}
    )

    def stream_res(response):
        for result in response.iter_lines(chunk_size=None):
            try:
                result_dict = orjson.loads(result.decode())
            except Exception as e:
                print("Failed to decode: %s", result)
                raise e
            yield result_dict["data"]

    return stream_res(response)


"""WARNING The functions below use the GRIP protobuf API directly and bipass Auth checks
            and should not be used for Gen3 Purposes."""


def proto_stream_query(grip_service: str, graph_name: str, query: dict) -> Generator[dict, None, None]:
    """Get all records for an vertex type.
        This function uses the internal protobuf API instead of the plugin API
            For example query dict for getting all of the Observation vertices:
                data = {
                        "query": [
                            {"v": []},
                            {"hasLabel": ["Observation"]}
                        ]
                    }
    """

    response = requests.post(
        f"http://{grip_service}:8201/{PROTOBUF_PATH}/{graph_name}/query",
        data=orjson.dumps(query),
    )

    def stream_protobuf_res(response):
        for result in response.iter_lines(chunk_size=None):
            try:
                result_dict = orjson.loads(result.decode())
            except Exception as e:
                print("Failed to decode: %s", result)
                raise e
            yield result_dict["vertex"]["data"]

    return stream_protobuf_res(response)


def list_labels(grip_service: str, graph_name: str) -> dict:
    """Get all of the edge and vertex labels for a given graph.
        Label names are based off of FHIR vertex and edge names
        Example response:
            {'vertexLabels':
                  ['BodyStructure', 'Condition', 'DocumentReference', 'Observation',
                   'Organization', 'Patient', 'ResearchStudy', 'ResearchSubject', 'Specimen'],
             'edgeLabels':
                  ['body_structure', 'condition', 'document_reference',
                   'focus_DocumentReference', 'focus_Specimen', 'focus_observation',
                   'parent', 'parent_specimen', 'partOf', 'partOf_research_study',
                   'patient', 'research_subject', 'specimen', 'specimen_Specimen',
                   'specimen_observation', 'study', 'subject_Patient',
                   'subject_Specimen', 'subject_observation']
            }
    """
    response = requests.get(
        f"http://{grip_service}:8201/{PROTOBUF_PATH}/{graph_name}/label"
    )
    return response.json()
