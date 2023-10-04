from enum import unique
from textwrap import indent
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
from ast import literal_eval


# Setup code for downloading the data
# A retry strategy is required to mitigate a temporary infrastructure 504 infrastructure issue.
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[requests.codes.gateway_timeout],
    allowed_methods=["HEAD", "GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
https = requests.Session()
https.mount("https://", adapter)

# Note that API versions have changed. Old dataset IDs in the APi response have the field 'dataset_id_version'.
CELLXGENE_PRODUCTION_ENDPOINT = 'https://api.cellxgene.cziscience.com'
COLLECTIONS = CELLXGENE_PRODUCTION_ENDPOINT + "/dp/v1/collections/"
DATASETS = CELLXGENE_PRODUCTION_ENDPOINT + "/dp/v1/datasets/"


# GET all the public collection UUIDs
r = https.get(COLLECTIONS)
r.raise_for_status()
collections = sorted(r.json()['collections'], key=lambda key: key['created_at'], reverse=True)

# Gather collection for each collection and its datasets
keep_collection = {'datasets', 'name'}
all_collections = []
for collection in collections:
    print('GET ' + COLLECTIONS + collection['id'])
    r1 = https.get(COLLECTIONS + collection['id'], timeout=5)
    r1.raise_for_status()
    collection_collection = r1.json()
    all_collections.append(collection_collection)


unique_dataset_links = {}

for collection in all_collections:
    primary_cell_count = 0
    secondary_cell_count = 0
    extra_cell_count = 0
    primary_dataset_links = {}
    secondary_dataset_links = {}

    for dataset in collection['datasets']:
        diseases = dataset['disease']
        for disease in diseases:
            if (str(disease['label']).lower() == 'normal' and str(dataset['organism'][0]['label']).lower() == 'homo sapiens'):
                for asset in dataset['dataset_assets']:
                    if asset['filetype'] == 'H5AD':
                        if dataset['is_primary_data'] == 'PRIMARY':
                            primary_cell_count += dataset['cell_count']
                            primary_dataset_links[dataset['id']] = {
                                'dataset_name'      : dataset['name'],
                                'is_primary_data'   : dataset['is_primary_data'],
                                'collection_id'     : dataset['collection_id'],
                                'donor_id'          : dataset['donor_id'],
                                'tissue'            : [tissue_dict['label'] for tissue_dict in dataset['tissue']]
                            }

                        elif dataset['is_primary_data'] == 'SECONDARY':
                            secondary_cell_count += dataset['cell_count']
                            secondary_dataset_links[dataset['id']] = {
                                'dataset_name'      : dataset['name'],
                                'is_primary_data'   : dataset['is_primary_data'],
                                'collection_id'     : dataset['collection_id'],
                                'donor_id'          : dataset['donor_id'],
                                'tissue'            : [tissue_dict['label'] for tissue_dict in dataset['tissue']]
                            }
                        else:
                            extra_cell_count += dataset['cell_count']

    # Condition 1 : CELL COUNTS : Primary > Secondary:
    if (primary_cell_count > secondary_cell_count) and (primary_cell_count > 0):
        unique_dataset_links = {**unique_dataset_links, **primary_dataset_links}

    # Condition 2 : CELL COUNTS : Primary < Secondary:
    elif (primary_cell_count < secondary_cell_count) and (secondary_cell_count > 0):
        unique_dataset_links = {**unique_dataset_links, **secondary_dataset_links}

data = pd.DataFrame(unique_dataset_links).transpose()
data['tissue'] = data['tissue'].apply(lambda x: literal_eval(str(x)))
data['donor_id'] = data['donor_id'].apply(lambda x: literal_eval(str(x)))
data = data.explode('donor_id').explode('tissue')
data['unique_dataset_id'] = data.apply(lambda row: COLLECTIONS + row['collection_id'] + '#' + '$'.join([row.name,row['donor_id'],row['tissue'].replace(' ','%20')]),axis=1)
data.to_csv('cellxgene.csv')