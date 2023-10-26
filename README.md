  
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

see etl pod

## Distribution

- PyPi

```
# update pypi

# pypi credentials - see https://twine.readthedocs.io/en/stable/#environment-variables

export TWINE_USERNAME=  #  the username to use for authentication to the repository.
export TWINE_PASSWORD=  # the password to use for authentication to the repository.

# this could be maintained as so: export $(cat .env | xargs)

rm -r dist/
python3  setup.py sdist bdist_wheel
twine upload dist/*
```