import requests
import os
from typing import List
import orjson


def bulk_add(graph_name: str, project_id: str, directory_path: str, output: dict, access_token: str) -> None:
    """Loads a directory of .ndjson or .gz files to grip.
        Args:
                graph_name: The name of the graph
                project_id: the Gen3 program-project to be used
                directory_path: the file path to the directory that contains the FHIR files

        TODO: implement FHIR schema in grip
                    so that edges that don't validate in graph are rejected
    """
    # List graphs and check to see if graph name is amoung the graphs listed
    exists = graph_exists(graph_name, output, access_token)
    assert exists, output["logs"].append(f"ERROR: graph {graph_name} not found in grip")

    output["logs"].append(f"loading files into {graph_name} from {directory_path}")

    assert os.path.isdir(directory_path), output["logs"].append(f"directory path {directory_path} is not a directory")
    importable_files = [f for f in os.listdir(directory_path) if any([f.endswith(".json"), f.endswith(".gz"), f.endswith(".ndjson")])]
    assert len(importable_files) > 0, output["logs"].append(f"No .json, .gz or .ndjson files have been uploaded")

    output["logs"].append(f"files in {directory_path}: {str(os.listdir(directory_path))}")
    output["logs"].append(f"importable files found: {str(importable_files)}")

    for file in importable_files:
        file_path = f"{directory_path}/{file}"
        output["logs"].append(f"loading file: {file_path}")
        graph_component = "edge" if "edge" in file_path else "vertex"
        with open(file_path, 'rb') as file_io:
            files = {'file': (file_path, file_io)}
            response = requests.post(
                f"http://local-grip:8201/graphql/{graph_name}/bulk-load/{project_id}",
                data={"types": graph_component},
                headers={"Authorization": f"bearer {access_token}"},
                files=files
            )

        response.raise_for_status()
        json_data = response.json()
        output["logs"].append(f"json data: {json_data}")


def bulk_delete(graph_name: str, vertices: List[str], project_id: str,  edges: List[str], output: dict, access_token: str) -> None:
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
    response = requests.delete(f"http://local-grip:8201/graphql/{graph_name}/bulk-delete/{project_id}",
                               data=orjson.dumps(data),
                               headers={"Authorization": f"bearer {access_token}"}
                               )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"bulk-delete response: {json_data}")


def delete_edge(graph_name: str, edge_id: str, project_id: str, output: dict, access_token: str) -> None:
    """Deletes one edge from the specified graph"""
    response = requests.delete(f"http://local-grip:8201/graphql/{graph_name}/del-edge/{project_id}/{edge_id}",
                               headers={"Authorization": f"bearer {access_token}"}
                               )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"del-edge response: {json_data}")


def delete_vertex(graph_name: str, vertex_id: str, project_id: str, output: dict, access_token: str) -> None:
    """Deletes one vertex from the specified graph"""
    response = requests.delete(f"http://local-grip:8201/graphql/{graph_name}/del-verex/{project_id}/{vertex_id}",
                               headers={"Authorization": f"bearer {access_token}"}
                               )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"del-vertex response: {json_data}")


def add_vertex(graph_name: str, vertex: dict, project_id: str, output: dict, access_token: str) -> None:
    """Adds one vertex to the specified graph
        required vertex format:
            {
                "gid": str, id of vertex,
                "label": str, resource type,
                "data": dict, vertex properties.
            }
    """
    response = requests.post(f"http://local-grip:8201/graphql/{graph_name}/add-verex/{project_id}/{vertex["gid"]}",
                             headers={"Authorization": f"bearer {access_token}"},
                             json=vertex
                             )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"add-vertex response: {json_data}")


def add_edge(graph_name: str, edge: dict, project_id: str, output: dict, access_token: str) -> None:
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
    response = requests.post(f"http://local-grip:8201/graphql/{graph_name}/add-verex/{project_id}/{edge["gid"]}",
                             headers={"Authorization": f"bearer {access_token}"},
                             json=edge
                             )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"add-edge response: {json_data}")


def list_graphs(output: dict, access_token: str) -> List[str]:
    """Returns a list of all graph names in grip"""
    response = requests.get("http://local-grip:8201/graphql/list-graphs",
                            headers={"Authorization": f"bearer {access_token}"}
                            )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"list-graphs response: {json_data}")

    assert "data" in json_data and "graphs" in json_data["data"], output["logs"].append("Expecting json_data['data']['graphs'] to be indexable")
    return json_data["data"]["graphs"]


def add_schema(graph_name: str, schema_path: str, project_id: str, output: dict, access_token: str) -> None:
    """Adds a schema to a graph in grip. NOTE: currently the schema that is attached to the graph
    is whatever graph is specified with the '"graph": "ESCA"' at the top of the schema file,
    not the graph_name that is specified"""

    assert os.path.isfile(schema_path), output["logs"].append(f"{schema_path} is not a file")
    with open(schema_path, 'rb') as file_io:
        files = {'file': (schema_path, file_io)}
        response = requests.post(
            f"http://local-grip:8201/graphql/{graph_name}/add-schema/{project_id}",
            headers={"Authorization": f"bearer {access_token}"},
            files=files
        )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"add-schema response: {json_data}")


def add_graph(graph_name: str, project_id: str, output: dict, access_token: str) -> None:
    """Creates a new graph"""

    exists = graph_exists(graph_name, output, access_token)
    if not exists:
        response = requests.post(f"http://local-grip:8201/graphql/{graph_name}/add-graph/{project_id}",
                                 headers={"Authorization": f"bearer {access_token}"})

        response.raise_for_status()
        json_data = response.json()
        output["logs"].append(f"add-graph response: {json_data}")
        return

    output["logs"].append(f"graph {graph_name} already exists")


def drop_graph(graph_name: str, project_id: str, output: dict, access_token: str) -> None:
    """Deletes a graph and all of its data"""

    exists = graph_exists(graph_name, output, access_token)
    # Not going to get a grip error if you attempt to delete something that doesn't exist it grip,
    # But it might be good to still have an assert statement here to catch the fact that the graph doesn't exist
    assert exists, output["logs"].append(f"Graph {graph_name} does not exist in grip")
    response = requests.delete(f"http://local-grip:8201/graphql/{graph_name}/del-graph/{project_id}",
                               headers={"Authorization": f"bearer {access_token}"})

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"del-graph response: {json_data}")


def graph_exists(graph_name: str, output: dict, access_token: str) -> bool:
    """Check to see if the provided graph name exists in grip"""
    existing_graphs = list_graphs(output, access_token)
    return graph_name in existing_graphs


def delete_project(graph_name: str, project_id: str, output: dict, access_token: str) -> None:
    """Delete a gen3 project entirely from a grip graph"""
    response = requests.delete(
            f"http://local-grip:8201/graphql/{graph_name}/proj-delete/{project_id}",
            headers={"Authorization": f"bearer {access_token}"},
            )

    response.raise_for_status()
    json_data = response.json()
    output["logs"].append(f"proj-delete response: {json_data}")
