import requests
import pandas as pd

HUBMAP_TOKEN = 'Ag8BgkBEN1jxnqDk08zdraOOgoYx0vDjWW8Mv7rvm7zElbqPPPF8C57JaoNMn6Eo1jDopNn4VEN032uydKrwvtNQjgo'
HUBMAP_SEARCH_ENDPOINT = 'https://search.api.hubmapconsortium.org/v3/portal/search'

def get_hubmap_datasets_info():
    header = { 'Content-type': 'application/json', 'Authorization': 'Bearer' + HUBMAP_TOKEN } if HUBMAP_TOKEN else { 'Content-type': 'application/json' }
    body = {
            'version': True,
            'from': 0,
            'size': 10000,
            'query': {
                    'term': {
                            'files.rel_path.keyword': 'raw_expr.h5ad',
                          },
                   },
             '_source': {
                        'includes': ['uuid', 'hubmap_id', 'data_types' , 'origin_samples_unique_mapped_organs'],
                      },
           }
    return requests.post(HUBMAP_SEARCH_ENDPOINT,headers=header,json=body).json()['hits']['hits']

if __name__ == "__main__":
    hubmap_dataset_info = [item['_source'] for item in get_hubmap_datasets_info()]
    pd.DataFrame(hubmap_dataset_info).to_csv('hubmap.csv')
    