
from __future__ import annotations

import uuid
from typing import Optional, Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

import logging

from fhir.resources.bundle import Bundle, BundleEntry, BundleEntryResponse
from fhir.resources.operationoutcome import OperationOutcome, OperationOutcomeIssue

logger = logging.getLogger(__name__)

valid_resource_types = [
    "ResearchStudy",
    "Patient",
    "ResearchSubject",
    "Substance",
    "Specimen",
    "Observation",
    "Condition",
    "Medication",
    "MedicationAdministration",
    "DocumentReference",
    "Task",
    "FamilyMemberHistory",
]

tags_metadata = [
    {
        "name": "System",
        "description": "Cluster operations, health, and status.",
    },
    {
        "name": "Submission",
        "description": "Manage **submission**, validate and populate backend data stores and portal.",
    },
]

app = FastAPI(
    title="ACED Submission",
    contact={},
    version="0.0.1",
    description="""ACED FHIR Bundle Implementation""",
    servers=[
        {
            "url": "https://aced-idp.org/Bundle",
            "description": "ACED FHIR Bundle Implementation",
        }
    ],
    openapi_tags=tags_metadata,
)


@app.post(
    "/Bundle",
    # response_model=Any,
    # responses={"default": {"model": Any}},
    status_code=201,
    tags=["Submission"],
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json+fhir": {
                    "schema": {
                        "type": "object",
                        "description": """FHIR [Bundle](https://hl7.org/fhir/R5/bundle.html)"""
                    }
                }
            }
        },
        "responses": {
            201: {
                "description": "Created",
                "content": {
                    "application/json+fhir": {
                        "schema": {
                            "type": "object",
                            "description": "FHIR [Bundle](https://hl7.org/fhir/R5/bundle.html)",
                        }
                    }
                },
            },
            422: {
                "description": "Unprocessable Entity",
                "content": {
                    "application/json+fhir": {
                        "schema": {
                            "type": "object",
                            "description": "FHIR [OperationOutcome](https://hl7.org/fhir/R5/operationoutcome.html) issues that apply to [Bundle](https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.issues) or [Entry](https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.entry.response.outcome)",
                        }
                    }
                },
            },
            401: {
                "description": "Security Error",
                "content": {
                    "application/json+fhir": {
                        "schema": {
                            "type": "object",
                            "description": "Authorization header issue",
                        }
                    }
                },
            },
        }
    },
)
async def post__bundle(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    body: Request = None,
) -> Any:
    """
    Import a FHIR Bundle.\n
    * In order to prevent "schema explosion" 🤯, the openapi definitions here are minimal, validation will use complete `R5 Bundle definitions` [here](https://hl7.org/fhir/R5/bundle.html).\n
    * The FHIR Bundle must be of [type](https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.type) `transaction` and contain a `https://aced-idp.org/project_id` identifier.\n
    * Bundle entry [method](https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.entry.request.method) must be of type `PUT` or `DELETE`.\n
    * Entry [resource](https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.entry.resource) must be one of the `supported types` [here](https://github.com/ACED-IDP/submission/wiki/Submission#valid-resource-types).\n
    * See more regarding `use case and validations` [here](https://github.com/ACED-IDP/submission/wiki/Submission).
    """

    body_dict = await body.json()

    # validate bundle as a whole
    outcome = validate_bundle(body_dict, authorization)

    # validate each entry in the bundle
    response_entries = validate_bundle_entries(body_dict)

    # set status code
    status_code = 201
    headers = {"Content-Type": "application/fhir+json"}
    for response_entry in response_entries:
        if response_entry.response.status != "200":
            status_code = 422
            break
    if outcome.issue:
        status_code = 422
        if len([_.code for _ in outcome.issue if _.code == "security"]):
            status_code = 401

    # TODO process each entry in the bundle, save request_bundle
    response = Bundle(
        type="transaction-response", entry=response_entries, issues=outcome
    )
    response.id = str(uuid.uuid4())
    Bundle.validate(response)

    if status_code == 201:
        headers["Location"] = f"https://aced-idp.org/Bundle/{response.id}"


    return JSONResponse(
        content=response.dict(), status_code=status_code, headers=headers
    )


def validate_entry(request_entry: BundleEntry) -> OperationOutcomeIssue:
    """Validate a single entry, return issue or None"""
    if request_entry.request.method not in ["PUT", "DELETE"]:
        return OperationOutcomeIssue(
            severity="error",
            code="invariant",
            diagnostics=f"Invalid entry.method {request_entry.request.method} for entry {request_entry.fullUrl}, must be PUT or DELETE",
        )
    resource_type = request_entry.resource.resource_type
    if resource_type not in valid_resource_types:
        return OperationOutcomeIssue(
            severity="error",
            code="invariant",
            diagnostics=f"Unsupported resource {resource_type}",
        )
    if not request_entry.resource.identifier:
        return OperationOutcomeIssue(
            severity="error", code="required", diagnostics="Resource missing identifier"
        )
    return OperationOutcomeIssue(
        severity="success",
        code="success",
        diagnostics="Valid entry",
    )


def validate_bundle_entries(body: dict) -> list[BundleEntry]:
    """Ensure bundle entries are valid for our use case, Messages relating to the processing of individual entries (e.g. in a batch or transaction) SHALL be reported in the entry.response.outcome for that entry.
    https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.issues
    raise HTTPException if not"""

    response_entries = []
    request_entries = body.get("entry", [])
    for entry_dict in request_entries:
        request_entry = BundleEntry(
            **entry_dict
        )  # TODO - this can be invalid, capture issue
        response_entry = BundleEntry()
        response_issue = validate_entry(request_entry)
        if response_issue.severity == "success":
            response_status = "200"
        else:
            response_status = "422"
        response_entry.response = BundleEntryResponse(status=response_status)
        response_entry.response.outcome = OperationOutcome(issue=[response_issue])
        response_entries.append(response_entry)

    return response_entries


def validate_bundle(body: dict, authorization: str) -> OperationOutcome:
    """Ensure bundle is valid for our use case, These issues and warnings must apply to the Bundle as a whole, not to individual entries.
    see https://hl7.org/fhir/R5/bundle-definitions.html#Bundle.issues
    """
    outcome = OperationOutcome(issue=[])
    if body is None or body == {}:
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="required",
                diagnostics="Bundle missing body",
            )
        )

    if authorization is None:
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="security",
                diagnostics="Missing Authorization header",
            )
        )

    _ = body.get("resourceType", None)
    if _ != "Bundle":
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="required",
                diagnostics=f"Body must be a FHIR Bundle, not {_}",
            )
        )

    _ = body.get("type", None)
    if _ != "transaction":
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="required",
                diagnostics=f"Bundle must be of type `transaction`, not {_}",
            )
        )

    identifier = body.get("identifier", None)
    project_id = None
    if identifier is None:
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="required",
                diagnostics="Bundle missing identifier",
            )
        )

    if (
        identifier
        and identifier.get("system", None) == "https://aced-idp.org/project_id"
    ):
        project_id = identifier.get("value", None)
    if not project_id:
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="required",
                diagnostics="Bundle missing identifier https://aced-idp.org/project_id",
            )
        )

    _ = body.get("entry", None)
    if _ is None or _ == []:
        outcome.issue.append(
            OperationOutcomeIssue(
                severity="error",
                code="required",
                diagnostics="Bundle missing entry",
            )
        )

    return outcome


@app.get("/_status", response_model=None, tags=["System"])
def get__status() -> None:
    """
    Returns if service is healthy or not
    """
    pass
