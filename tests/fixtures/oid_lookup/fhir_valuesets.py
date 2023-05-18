from orjson import orjson

"""Prepare json objects for simplifier"""

# https://github.com/nazrulworld/hl7-archives/blob/0.4.0/FHIR/R5/5.0.0-definitions.json.zip
with open('tests/fixtures/oid_lookup/valuesets.json', "rb") as fp:
    value_sets = orjson.loads(fp.read())


fhir_valuesets = {}

fhir_valuesets_old = {}

for value_set in value_sets['entry']:
    value_set = value_set['resource']
    id_ = value_set['id']

    if 'identifier' not in value_set:
        continue

    identifiers = list(set([_['value'] for _ in value_set['identifier']]))
    old_identifiers = []
    for _ in value_set['identifier']:
        if _.get('use', None) == 'old':
            identifiers.remove(_['value'])
            old_identifiers.append(_['value'])
    if len(identifiers) > 0:
        for identifier in identifiers:
            fhir_valuesets[identifier] = id_
    if len(old_identifiers) > 0:
        for identifier in old_identifiers:
            fhir_valuesets_old[identifier] = id_


# update oid_lookup fhir_valuesets
b_: bytes = orjson.dumps(fhir_valuesets)
print(b_.decode())

b_: bytes = orjson.dumps(fhir_valuesets_old)
print(b_.decode())
