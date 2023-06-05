  
# submission

Utilities to upload metadata and files to ACED's Gen3 instance

## Setup

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e . 
```

## Use

### Upload files
```
study=Alcoholism
aced_submission files upload  --program aced --project $study --bucket_name $study'_BUCKET' --document_reference_path studies/$study
```

### Upload metadata
```commandline
aced_submission meta upload  --program aced --project Alcoholism $Alcoholism_BUCKET --document_reference_path studies/Alcoholism

```

## Test

* fixtures

```
TODO --
```
