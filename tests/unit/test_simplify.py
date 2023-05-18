import pathlib

import orjson

from aced_submission.simplifier import simplify_directory, validate_simplified_value


def test_simplify_study():
    """Ensure we can validate a synthetic study"""
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
                if simplified['resourceType'] == 'DocumentReference':
                    expected = set(['content_resourceType', 'content_md5', 'content_contentType', 'content_url', 'content_size'])
                    actual = set([_ for _ in simplified if _.startswith('content')])
                    assert expected == actual, (file_name, line)
