import pathlib

from aced_submission.util import directory_reader
from aced_submission.validator import validate_directory


def test_validate_directory_files(directory_path=pathlib.Path('tests/fixtures/valid-files')):
    """Ensure valid json rendered from files."""
    results = validate_directory(directory_path)
    assert len(results.exceptions) == 0, f"Did not expect exceptions {results.exceptions}"
    assert len(results.resources) == 5, f"Expected 5 resources {results.resources}"


def test_validate_directory_zips(directory_path=pathlib.Path('tests/fixtures/valid-zips')):
    """Ensure valid json rendered from gz."""
    results = validate_directory(directory_path)
    assert len(results.exceptions) == 0, f"Did not expect exceptions {results.exceptions}"
    assert len(results.resources) == 5, f"Expected 5 resources {results.resources}"


def test_validate_invalid_files(directory_path=pathlib.Path('tests/fixtures/invalid-files')):
    """Ensure invalid json is captured."""
    results = validate_directory(directory_path)
    assert len(results.exceptions) == 4, f"Expected exceptions {results.exceptions}"


def test_validate_pattern(directory_path=pathlib.Path('tests/fixtures/valid-files'), pattern="bundle.json"):
    for result in directory_reader(directory_path, pattern):
        assert result.offset is not None, "Expected offset"
        assert result.resource, "Expected resource"
        assert result.exception is None, "Unexpected exception"
