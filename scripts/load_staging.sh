
#!/bin/bash
set -e
# Any subsequent(*) commands which fail will cause the shell script to exit immediately


# assign them to projects
export Alcoholism_BUCKET=aced-default-staging 
export Alzheimers_BUCKET=aced-ucl-staging 
export Breast_Cancer_BUCKET=aced-manchester-staging 
export Colon_Cancer_BUCKET=aced-stanford-staging 
export Diabetes_BUCKET=aced-ucl-staging 
export Lung_Cancer_BUCKET=aced-manchester-staging 
export Prostate_Cancer_BUCKET=aced-stanford-staging 
export NVIDIA_BUCKET=aced-default-staging 



export studies=(Alzheimers Breast_Cancer Colon_Cancer Diabetes Lung_Cancer Prostate_Cancer)

# now load it all up

echo "Load all studies"

for study in ${studies[@]}; do
  aced_submission files upload --program aced --project $study --bucket_name `eval "echo \\$${study}_BUCKET"`  --document_reference_path studies/$study  --duplicate_check 
  aced_submission meta graph upload --source_path studies/$study/extractions/ --program aced --project $study  --dictionary_path https://aced-public.s3.us-west-2.amazonaws.com/aced-test.json
  aced_submission meta flat denormalize-patient --input_path studies/$$study/extractions/Patient.ndjson
  aced_submission meta flat load --project_id aced-$study --index patient --path studies/$study/extractions/Patient.ndjson --schema_path  https://aced-public.s3.us-west-2.amazonaws.com/aced-test.json
  aced_submission meta flat load --project_id aced-$study --index patient --path studies/$study/extractions/Patient.ndjson --schema_path  https://aced-public.s3.us-west-2.amazonaws.com/aced-test.json
  aced_submission meta flat load --project_id aced-$study --index patient --path studies/$study/extractions/Patient.ndjson --schema_path  https://aced-public.s3.us-west-2.amazonaws.com/aced-test.json
done


