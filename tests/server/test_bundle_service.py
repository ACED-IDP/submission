import copy

from fastapi.testclient import TestClient
from requests import Response

from bundle_service.main import app

client = TestClient(app)

HEADERS = {"Authorization": "foo"}

VALID_CLAIM = {
    "resourceType": "Claim",
    "status": "active",
    "created": "2014-08-16",
    "use": "claim",
    "type": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                "code": "oral",
            }
        ]
    },
    "patient": {"reference": "Patient/1"},
}

VALID_PATIENT = {
    "resourceType": "Patient",
    "identifier": [{"system": "https://example.org/my_id", "value": "test-foo"}],
}

VALID_REQUEST_BUNDLE = {
    "resourceType": "Bundle",
    "type": "transaction",
    "identifier": {"system": "https://aced-idp.org/project_id", "value": "test-foo"},
    "entry": [
        {
            "resource": None,
            "request": {"method": "PUT", "url": "Claim"},
        }
    ],
}


def test_read_main():
    """The main page should return a 404."""
    response = client.get("/")
    assert response.status_code == 404, response.status_code


def test_read_health():
    """The health page should return a 200."""
    response = client.get("/_status")
    assert response.status_code == 200, response.status_code


def test_read_bundle():
    """A GET bundle page should return a 405."""
    response = client.get("/Bundle")
    assert response.status_code == 405, response.status_code


def assert_bundle_response(
    response: Response,
    expected_status_code: int,
    bundle_diagnostic: str = None,
    entry_diagnostic: str = None,
):
    """Check that a bundle response is valid."""
    assert response.status_code == expected_status_code, response.status_code
    response_bundle = response.json()
    assert "resourceType" in response_bundle, response_bundle
    assert response_bundle["resourceType"] == "Bundle", response_bundle
    assert response_bundle["type"] == "transaction-response", response_bundle
    response_bundle["issues"]["resourceType"] == "OperationOutcome", response_bundle[
        "issues"
    ]
    # print(_)
    if bundle_diagnostic:
        actual_bundle_diagnostic = sorted(
            [_["diagnostics"] for _ in response_bundle["issues"]["issue"]]
        )
        assert bundle_diagnostic in actual_bundle_diagnostic, response_bundle
    if entry_diagnostic:
        actual_entry_diagnostic = sorted(
            [
                _["diagnostics"]
                for _ in response_bundle["entry"][0]["response"]["outcome"]["issue"]
            ]
        )
        assert entry_diagnostic in actual_entry_diagnostic, response_bundle


def create_request_bundle(
    bundle: dict = VALID_REQUEST_BUNDLE, resource: dict = VALID_PATIENT
) -> dict:
    """create a bundle request."""
    _ = copy.deepcopy(bundle)
    _["entry"][0]["resource"] = resource
    return _


def test_write_bundle_no_data():
    """A POST bundle without data should return a 422."""
    response = client.post("/Bundle", json={}, headers=HEADERS)
    assert_bundle_response(response, 422, bundle_diagnostic="Bundle missing body")


def test_write_bundle_no_auth():
    """A POST bundle with data, but no Auth header should return a 401."""
    response = client.post("/Bundle", json={"resourceType": "Bundle"})
    assert_bundle_response(
        response, 401, bundle_diagnostic="Missing Authorization header"
    )


def test_write_misc_resource():
    """A POST bundle with data, but not a Bundle should return a 422."""
    response = client.post("/Bundle", json={"resourceType": "Foo"}, headers=HEADERS)
    assert_bundle_response(
        response, 422, bundle_diagnostic="Body must be a FHIR Bundle, not Foo"
    )


def test_write_bundle_missing_entry():
    """A POST bundle missing `entry` should return a 422."""
    request_bundle = create_request_bundle()
    del request_bundle["entry"]
    response = client.post("/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(response, 422, bundle_diagnostic="Bundle missing entry")

    request_bundle = create_request_bundle()
    request_bundle["entry"] = []
    response = client.post("/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(response, 422, bundle_diagnostic="Bundle missing entry")


def test_write_bundle_missing_identifier():
    """A POST bundle missing `identifier` should return a 422."""
    request_bundle = create_request_bundle()
    del request_bundle["identifier"]
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(response, 422, bundle_diagnostic="Bundle missing identifier")

    request_bundle = create_request_bundle()
    request_bundle["identifier"] = {"system": "https://foo.bar", "value": "foo"}
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(
        response,
        422,
        bundle_diagnostic="Bundle missing identifier https://aced-idp.org/project_id",
    )


def test_write_bundle_incorrect_method():
    """A POST bundle entry without PUT or DELETE should return a 422."""
    request_bundle = create_request_bundle()
    request_bundle["entry"][0]["request"]["method"] = "POST"
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(
        response,
        422,
        entry_diagnostic="Invalid entry.method POST for entry None, must be PUT or DELETE",
    )


def test_write_bundle_unsupported_resource():
    """A POST bundle entry without an unsupported resource should return a 422."""
    request_bundle = create_request_bundle(resource=VALID_CLAIM)
    import pprint

    pprint.pprint(request_bundle)
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(response, 422, entry_diagnostic="Unsupported resource Claim")


def test_write_bundle_patient_missing_identifier():
    """A POST bundle entry.resource without identifier should produce 422."""
    request_bundle = create_request_bundle(resource={"resourceType": "Patient"})
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(
        response, 422, entry_diagnostic="Resource missing identifier"
    )


def test_write_bundle_simple_ok():
    """A POST bundle without type should produce 201."""
    request_bundle = create_request_bundle()
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(response, 201)
    response_bundle = response.json()
    assert response_bundle["entry"][0]["response"]["status"] in [
        "200",
        "201",
    ], response_bundle
    response.headers[
        "Location"
    ] == f'https://aced-idp.org/Bundle/{response_bundle["id"]}', "Response header Location should be set to the new Bundle ID"


def test_write_bundle_missing_type():
    """A POST bundle without type should produce 422."""
    request_bundle = create_request_bundle()
    del request_bundle["type"]
    response = client.post(url="/Bundle", json=request_bundle, headers=HEADERS)
    assert_bundle_response(
        response,
        422,
        bundle_diagnostic="Bundle must be of type `transaction`, not None",
    )


def test_openapi_ui():
    response = client.get(url="/redoc")
    assert response.status_code == 200, response.status_code


def test_openapi_json():
    response = client.get(url="/openapi.json")
    assert response.status_code == 200, response.status_code
    assert response.json()
