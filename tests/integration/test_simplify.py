import pathlib

import orjson

from aced_submission.simplifier import simplify_directory, validate_simplified_value


def test_simplify_fhir_samples():

    simplify_directory('tests/fixtures/simplify/5.0.0-examples-json/', '**/*.*', 'tmp/5.0.0-examples-json/extractions', 'https://aced-public.s3.us-west-2.amazonaws.com/aced.json', 'FHIR')
    """--input_path tests/fixtures/simplify/5.0.0-examples-json/ --output_path tmp/extractions
    """
    directory_path = pathlib.Path('tmp/5.0.0-examples-json/extractions')
    input_files = [_ for _ in directory_path.glob("*.ndjson")]
    for file_name in input_files:
        with open(file_name) as fp:
            for line in fp.readlines():
                simplified = orjson.loads(line)
                all_ok = all([validate_simplified_value(_) for _ in simplified.values()])
                assert all_ok, (file_name, line)


def test_simplify_study():

    simplify_directory('tests/fixtures/simplify/study/', '**/*.*', 'tmp/study/extractions', 'https://aced-public.s3.us-west-2.amazonaws.com/aced.json', 'FHIR')
    """--input_path tests/fixtures/simplify/5.0.0-examples-json/ --output_path tmp/extractions
    """
    directory_path = pathlib.Path('tmp/study/extractions')
    input_files = [_ for _ in directory_path.glob("*.ndjson")]
    for file_name in input_files:
        with open(file_name) as fp:
            for line in fp.readlines():
                simplified = orjson.loads(line)
                all_ok = all([validate_simplified_value(_) for _ in simplified.values()])
                assert all_ok, (file_name, line)
