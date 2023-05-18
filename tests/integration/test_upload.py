#  with a running Gen3
cmd = """
GEN3SDK_MAX_RETRIES=1 aced_submission files upload --document_reference_path tmp/aced-foo/DocumentReference.ndjson  --program aced --project Alcoholism --bucket_name aced-ohsu --duplicate_check  --ignore_state
"""  # TODO
