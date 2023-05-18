import re
from aced_submission.util import parse_obj


def test_invalid_dict():
    """Should reject missing resourceType"""
    parse_result = parse_obj({})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Dict missing `resourceType`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_valid_dict():
    """Should accept resourceType"""
    parse_result = parse_obj({"resourceType": "Patient"})
    assert parse_result.resource, "Should return a resource"
    assert parse_result.exception is None, "Should not have returned an exception"
    assert parse_result.resource.resource_type == 'Patient', "Should have returned a Patient"


def test_invalid_identifier_value():
    """Should reject identifier.value"""
    parse_result = parse_obj({"resourceType": "Patient", "identifier": [{"value": None}]})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Missing `value`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_invalid_identifier_system():
    """Should reject identifier.system"""
    parse_result = parse_obj({"resourceType": "Patient", "identifier": [{"value": "foo"}]})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Missing `system`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"
    parse_result = parse_obj({"resourceType": "Patient", "identifier": [{"value": "foo", "system": "bar"}]})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"`system` is not a URI",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"
    parse_result = parse_obj({"resourceType": "Patient", "identifier": [{"value": "foo", "system": "http://%2F%2Ffoobar"}]})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"`system` should be a simple url without uuencoding",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_valid_identifier():
    """Should accept valid identifier"""
    parse_result = parse_obj({"resourceType": "Patient", "identifier": [{"value": "foo", "system": "http://bar"}]})
    assert parse_result.resource, "Should return a resource"
    assert parse_result.exception is None, "Should not have returned an exception"
    assert parse_result.resource.resource_type == 'Patient', "Should have returned a Patient"
    assert len(parse_result.resource.identifier) == 1, "Should have returned a Patient with identifier"


def test_invalid_coding_value():
    """Should reject coding.code."""
    parse_result = parse_obj({"resourceType": "Patient", "maritalStatus": {"coding": [{"code": None}]}})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Missing `code`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_invalid_coding_system():
    """Should reject coding.system."""
    parse_result = parse_obj({"resourceType": "Patient", "maritalStatus": {"coding": [{"code": "foo"}]}})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Missing `system`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"

    parse_result = parse_obj(
        {"resourceType": "Patient", "maritalStatus": {"coding": [{"code": "foo", "system": "bar"}]}})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"`system` is not a URI",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"

    parse_result = parse_obj(
        {"resourceType": "Patient", "maritalStatus": {"coding": [{"code": "foo", "system": "http://%2F%2Ffoobar"}]}})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"`system` should be a simple url without uuencoding",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_invalid_coding_display():
    """Should reject coding.system."""
    parse_result = parse_obj(
        {"resourceType": "Patient",
         "maritalStatus": {"coding": [{"code": "foo", "system": "http://foobar"}]}})
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Missing `display`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_valid_coding_system():
    """Should accept valid coding"""
    parse_result = parse_obj(
        {"resourceType": "Patient",
         "maritalStatus": {"coding": [{"code": "foo", "system": "http://foobar", "display": "fubar"}]}})
    assert parse_result.resource, "Should return a resource"
    assert parse_result.exception is None, "Should not have returned an exception"
    assert parse_result.resource.resource_type == 'Patient', "Should have returned a Patient"
    assert len(parse_result.resource.maritalStatus.coding) == 1, "Should have returned a Patient with maritalStatus"


def test_invalid_reference_no_reference():
    """Should accept valid reference"""
    parse_result = parse_obj(
        {
            "resourceType": "Patient",
            "managingOrganization": {
                "display": "Acme"
            }
        }
    )
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Missing `reference`",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_invalid_reference_absolute_reference():
    """Should accept valid reference"""
    parse_result = parse_obj(
        {
            "resourceType": "Patient",
            "managingOrganization": {
                "reference": "http://orgs/Acme"
            }
        }
    )
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Absolute references not supported",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_invalid_reference_urn():
    """Should accept valid reference"""
    parse_result = parse_obj(
        {
            "resourceType": "Patient",
            "managingOrganization": {
                "reference": "urn:org:acme"
            }
        }
    )
    assert parse_result.resource is None, "Should not return a resource"
    assert parse_result.exception, "Should have returned an exception"
    assert re.match(r"Does not appear to be Relative reference",
                    str(parse_result.exception)), f"Did not expect {parse_result.exception}"


def test_valid_reference():
    """Should accept valid reference"""
    parse_result = parse_obj(
        {
            "resourceType": "Patient",
            "managingOrganization": {
                "reference": "Organization/1"
            }
        }
    )
    assert parse_result.resource, "Should return a resource"
    assert parse_result.exception is None, "Should not have returned an exception"
    assert parse_result.resource.resource_type == 'Patient', "Should have returned a Patient"
    assert parse_result.resource.managingOrganization.reference, "Should have returned a Patient with managingOrganization"
