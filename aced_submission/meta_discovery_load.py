import os
import pathlib
import logging
import requests
from gen3.auth import Gen3Auth
from gen3.metadata import Gen3Metadata


# copied from gen3_util/config/__init__.py
def ensure_auth(refresh_file: [pathlib.Path, str] = None, validate: bool = False) -> Gen3Auth:
    """Confirm connection to Gen3 using their conventions.

    Args:
        refresh_file (pathlib.Path): The file containing the downloaded JSON web token.
        validate: check the connection by getting a new token

    """

    try:
        if 'ACCESS_TOKEN' in os.environ:
            access_token = os.environ
            auth = Gen3Auth(refresh_file=f"accesstoken:///{access_token}")

        elif refresh_file:
            if isinstance(refresh_file, str):
                refresh_file = pathlib.Path(refresh_file)
            auth = Gen3Auth(refresh_file=refresh_file.name)

        else:
            auth = Gen3Auth()

        if validate:
            api_key = auth.refresh_access_token()
            assert api_key, "refresh_access_token failed"

    except (requests.exceptions.ConnectionError, AssertionError) as e:
        msg = ("Could not get access. "
               "See https://uc-cdis.github.io/gen3-user-doc/appendices/api-gen3/#credentials-to-query-the-api. "
               "Store the file in ~/.gen3/credentials.json or specify location with env GEN3_API_KEY "
               f"{e}")

        logging.getLogger(__name__).error(msg)
        raise AssertionError(msg)

    return auth


def discovery_load(program, gen3_credentials_file):
    """Writes project information to discovery metadata-service"""
    auth = ensure_auth(gen3_credentials_file)
    discovery_client = Gen3Metadata(auth.endpoint, auth)
    # TODO - read from some other, more dynamic source
    discovery_descriptions = """
Alcoholism~9300~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Alcoholism.  Data hosted by: aced-ohsu~aced-ohsu
Alzheimers~45306~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Alzheimer's, Familial Alzheimer's.  Data hosted by: aced-ucl~aced-ucl
Breast_Cancer~7105~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Malignant neoplasm of breast (disorder).  Data hosted by: aced-manchester~aced-manchester
Colon_Cancer~25355~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Malignant tumor of colon,  Polyp of colon.  Data hosted by: aced-stanford~aced-stanford
Diabetes~65051~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Diabetes.  Data hosted by: aced-ucl~aced-ucl
Lung_Cancer~25355~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Non-small cell carcinoma of lung,TNM stage 1,  Non-small cell lung cancer, Suspected lung cancer.  Data hosted by: aced-manchester~aced-manchester
Prostate_Cancer~35488~Patients from 'Coherent Data Set' https://www.mdpi.com/2079-9292/11/8/1199/htm that were diagnosed with condition(s) of: Metastasis from malignant tumor of prostate, Neoplasm of prostate, arcinoma in situ of prostate.  Data hosted by: aced-stanford~aced-stanford""".split('\n')  # noqa E501

    for line in discovery_descriptions:
        if len(line) == 0:
            continue
        (name, _subjects_count, description, location,) = line.split('~')
        gen3_discovery = {'tags': [
            {"name": program, "category": "Program"},
            {"name": f"aced_{name}", "category": "Study Registration"},
            {"name": location, "category": "Study Location"},

        ], 'name': name, 'full_name': name, 'study_description': description}

        guid = f"aced_{name}"

        gen3_discovery['commons'] = "ACED"
        gen3_discovery['commons_name'] = "ACED Commons"
        # TODO - read this value for commons_url from some other, more dynamic source
        gen3_discovery['commons_url'] = 'staging.aced-idp.org'
        gen3_discovery['__manifest'] = 0
        gen3_discovery['_research_subject_count'] = int(_subjects_count)
        gen3_discovery['_unique_id'] = guid
        gen3_discovery['study_id'] = guid
        discoverable_data = dict(_guid_type="discovery_metadata", gen3_discovery=gen3_discovery)
        discovery_client.create(guid, discoverable_data, aliases=None, overwrite=True)
        print(f"Added {name}")
