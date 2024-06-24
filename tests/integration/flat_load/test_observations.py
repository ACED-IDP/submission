import pathlib
import pytest

from gen3_tracker.meta.dataframer import LocalFHIRDatabase

from aced_submission.meta_flat_load import load_flat, DEFAULT_ELASTIC


@pytest.fixture()
def project_id() -> str:
    return "test-observations"

@pytest.fixture()
def meta_dir() -> str:
    return "tests/fixtures/Breast_Cancer/META"


def test_load_observations(meta_dir, project_id, tmpdir) -> None:
    """Ensure we can load observations, the ES mapping should reflect updated fields."""
    # change to the temporary directory
    print(pathlib.Path.cwd())
    print(project_id)

    # create a database from META
    work_path = tmpdir
    assert pathlib.Path(work_path).exists(), f"Directory {work_path} does not exist."
    work_path = pathlib.Path(work_path)
    db_path = (work_path / "local_fhir.db")
    db_path.unlink(missing_ok=True)

    db = LocalFHIRDatabase(db_name=db_path)
    db.load_ndjson_from_dir(path=meta_dir)

    load_flat(project_id=project_id,
              generator=db.flattened_observations(),
              index='observation',
              limit=None,
              elastic_url=DEFAULT_ELASTIC,
              output_path=None
              )

    from opensearchpy import OpenSearch as Elasticsearch
    elastic = Elasticsearch([DEFAULT_ELASTIC], request_timeout=120)
    # retrieve by alias
    alias_mapping = elastic.indices.get_mapping(index='observation')
    # retrieve by index name
    index_mapping = elastic.indices.get_mapping(index='gen3.aced.io_observation_0')
    assert alias_mapping == index_mapping, "Alias mapping should match index mapping."
    # retrieve array config
    array_config_mapping = elastic.indices.get_mapping(index='gen3.aced.io_observation-array-config_0')
    assert array_config_mapping, "Array config mapping should exist."
    # no fields are arrays currently
    results = elastic.search(index='gen3.aced.io_observation-array-config_0')
    print(results)
    assert len(results['hits']['hits']) > 0, "Expected to find array config."
    assert len(results['hits']['hits'][0]['_source']['array']) == 2, "Expected to find 2 array fields."

    # aggregate by auth_resource_path
    # Define the query
    program, project = project_id.split('-')
    query = {
        "query": {
            "match": {
                "auth_resource_path": f"/programs/{program}/projects/{project}"
            }
        }
    }

    # Count the documents
    count = elastic.count(index="observation", body=query)

    assert count['count'] > 0, "Expected to find some observations."

    #
    # now lets make sure when we add a new field, it gets added to the mappingq
    #

    # add a new observation, with a new field `patient_us_core_favorite_ice_cream`
    observation = {"resourceType": "Observation", "id": "5f5454dd-XXXX-4bd9-YYYY-b4318ae469c3", "status": "final",
                   "category": "Laboratory",
                   "code": "Progesterone receptor Ag [Presence] in Breast cancer specimen by Immune stain",
                   "subject": "Patient/52e0c68f-8f6d-42f4-922c-0e32559d5a2b",
                   "effectiveDateTime": "2004-11-12T17:33:16-05:00", "issued": "2004-11-12T17:33:16.515-05:00",
                   "identifier": None, "value_normalized": "Positive (qualifier value)", "value_numeric": None,
                   "patient": "52e0c68f-8f6d-42f4-922c-0e32559d5a2b",
                   "patient_us_core_ethnicity": "Non Hispanic or Latino",
                   "patient_us_core_favorite_ice_cream": "Vanilla",
                   "patient_us_core_race": "Black or African American", "patient_us_core_birthsex": "F",
                   "project_id": "test-observations", "auth_resource_path": "/programs/test/projects/observations"}

    load_flat(project_id=project_id,
              generator=[observation],
              index='observation',
              limit=None,
              elastic_url=DEFAULT_ELASTIC,
              output_path=None
              )

    # retrieve by alias
    alias_mapping = elastic.indices.get_mapping(index='observation')
    assert alias_mapping['gen3.aced.io_observation_0']['mappings']['properties']['patient_us_core_favorite_ice_cream'] == {'type': 'keyword'}, "Expected to find the new field in the mapping."

    # we should find it by id
    doc = elastic.get(index='observation', id='5f5454dd-XXXX-4bd9-YYYY-b4318ae469c3')
    assert doc['_source']['patient_us_core_favorite_ice_cream'] == "Vanilla", "Expected to find the new field in the document."

    #
    # test the array_fields
    #
    # add a new observation, with a new field that is an array `patient_us_core_favorite_colors`
    observation = {"resourceType": "Observation", "id": "5f5454dd-ZZZZ-4bd9-DDDDD-b4318ae469c3", "status": "final",
                   "category": "Laboratory",
                   "code": "Progesterone receptor Ag [Presence] in Breast cancer specimen by Immune stain",
                   "subject": "Patient/52e0c68f-8f6d-42f4-922c-0e32559d5a2b",
                   "effectiveDateTime": "2004-11-12T17:33:16-05:00", "issued": "2004-11-12T17:33:16.515-05:00",
                   "identifier": None, "value_normalized": "Positive (qualifier value)", "value_numeric": None,
                   "patient": "52e0c68f-8f6d-42f4-922c-0e32559d5a2b",
                   "patient_us_core_ethnicity": "Non Hispanic or Latino",
                   "patient_us_core_favorite_ice_cream": "Vanilla",
                   "patient_us_core_favorite_colors": ["Red", "Green"],
                   "patient_us_core_race": "Black or African American", "patient_us_core_birthsex": "F",
                   "project_id": "test-observations", "auth_resource_path": "/programs/test/projects/observations"}

    load_flat(project_id=project_id,
              generator=[observation],
              index='observation',
              limit=None,
              elastic_url=DEFAULT_ELASTIC,
              output_path=None
              )

    # retrieve by alias
    alias_mapping = elastic.indices.get_mapping(index='observation')
    assert alias_mapping['gen3.aced.io_observation_0']['mappings']['properties']['patient_us_core_favorite_colors'] == {'type': 'keyword'}, "Expected to find the new field in the mapping."

    # we should find it by id
    doc = elastic.get(index='observation', id='5f5454dd-ZZZZ-4bd9-DDDDD-b4318ae469c3')
    assert doc['_source']['patient_us_core_favorite_colors'] == ["Red", "Green"], "Expected to find the new field in the document."

    results = elastic.search(index='gen3.aced.io_observation-array-config_0')
    print(results)
    assert len(results['hits']['hits']) > 0, "Expected to find array config."
    assert 'patient_us_core_favorite_colors' in results['hits']['hits'][0]['_source']['array']

