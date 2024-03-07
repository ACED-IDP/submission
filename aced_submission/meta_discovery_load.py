import os
import pathlib
import jwt
import logging
import requests
from gen3.auth import Gen3Auth
from gen3.metadata import Gen3Metadata
from gen3.submission import Gen3Submission


# copied from gen3_util/config/__init__.py
def ensure_auth(refresh_file: [pathlib.Path, str] = None, validate: bool = False) -> Gen3Auth:
    """Confirm connection to Gen3 using their conventions.

    Args:
        refresh_file (pathlib.Path): The file containing the downloaded JSON web token.
        validate: check the connection by getting a new token

    """

    try:
        if refresh_file:
            if isinstance(refresh_file, str):
                refresh_file = pathlib.Path(refresh_file)
            auth = Gen3Auth(refresh_file=refresh_file.name)
        elif 'ACCESS_TOKEN' in os.environ:
            auth = Gen3Auth(refresh_file=f"accesstoken:///{os.getenv('ACCESS_TOKEN')}")
        else:
            auth = Gen3Auth()

        if validate:
            api_key = auth.refresh_access_token()
            assert api_key, "refresh_access_token failed"

    except (requests.exceptions.ConnectionError, AssertionError) as e:
        msg = (f"Could not get access."
               "See https://bit.ly/3NbKGi4, or, "
               "store the file in ~/.gen3/credentials.json or specify location with env GEN3_API_KEY "
               f"{e}")

        logging.getLogger(__name__).error(msg)
        raise AssertionError(msg)

    return auth


def discovery_get(project_id: str):
    """Fetches project information from discovery metadata-service"""

    auth = ensure_auth()
    discovery_client = Gen3Metadata(auth.endpoint, auth)

    try:
        data = discovery_client.get(project_id)
    except requests.exceptions.HTTPError as e:
        print(str(e))
        if e.response.status_code == 404:
            return {}
        return None

    return data


def discovery_delete(project_id: str):
    """Deletes project information to discovery metadata-service"""

    auth = ensure_auth()
    discovery_client = Gen3Metadata(auth.endpoint, auth)

    try:
        discovery_client.delete(project_id)
        print(f"Deleted {project_id}")
    except requests.exceptions.HTTPError as e:
        print(str(e))


def discovery_load(project_id: str, _subjects_count: int, description: str, location: str, overwrite: bool):
    """Writes project information to discovery metadata-service.
       Overwrites existing data"""

    program, project = project_id.split("-")
    auth = ensure_auth()
    token = auth.get_access_token()

    # Decode the jwt ACCESS_TOKEN to get the commons endpoint
    decoded_token = jwt.decode(token, secret=None, algorithms=["RS256"], options={"verify_signature":False})
    commons_url = decoded_token["iss"].removesuffix("/user").removeprefix("https://")

    discovery_client = Gen3Metadata(auth.endpoint, auth)
    gen3_discovery = {'tags': [
        {"name": program, "category": "Program"},
        {"name": project, "category": "Project"},
        {"name": project_id, "category": "Study Registration"},
        {"name": location, "category": "Study Location"},

    ], 'name': project, 'full_name': project, 'study_description': description}

    gen3_discovery['commons'] = "ACED"
    gen3_discovery['commons_name'] = "ACED Commons"
    gen3_discovery['commons_url'] = commons_url
    gen3_discovery['__manifest'] = 0
    gen3_discovery['_research_subject_count'] = int(_subjects_count)
    gen3_discovery['_unique_id'] = project_id
    gen3_discovery['study_id'] = project_id
    discoverable_data = dict(_guid_type="discovery_metadata", gen3_discovery=gen3_discovery)

    try:
        discovery_client.create(project_id, discoverable_data, aliases=None, overwrite=overwrite)
        print(f"Added {project_id, discoverable_data}")
    except requests.exceptions.HTTPError as e:
        print(str(e))


def sync(overwrite: bool) -> bool:
    """Synchronizes legacy projects with researchStudies already
    in Elastic/Peregrine to the Discovery Page"""

    auth = ensure_auth()
    submission_client = Gen3Submission(auth)
    research_study_query = "{research_study(first: 10000000){\n    project_id\n    description\n    identifier_coding}}"

    """Needs to be a seperate query because research_study{_research_subject_count} query returns 0 in production"""
    research_subjects = "{research_subject(first: 10000000){project_id}}"

    study_records = submission_client.query(research_study_query)["data"]["research_study"]
    assert len(study_records) > 0

    subject_records = submission_client.query(research_subjects)["data"]["research_subject"]
    assert len(subject_records) > 0

    project_count = {}
    for item in subject_records:
        project_id = item['project_id']
        project_count[project_id] = project_count.get(project_id, 0) + 1

    for study in study_records:

        if study["project_id"] not in project_count:
            print(f"research subjects count for {study['project_id']} not found. Skipping...")
            continue

        discovery_load(study["project_id"], project_count[study["project_id"]], study["description"], study["identifier_coding"][0], overwrite)

    return True
