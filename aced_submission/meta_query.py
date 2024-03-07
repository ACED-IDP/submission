from elasticsearch import Elasticsearch


def counts_project_index(elastic: Elasticsearch, project_id: str, index: str) -> int:
    query = {
         "query": {
             "bool": {
                 "must": [
                     {"match": {"project_id": project_id}}
                 ]
             }
         }
    }
    results = elastic.search(index=index, body=query, size=0)
    _index_count = results['hits']['total']['value']

    return _index_count
