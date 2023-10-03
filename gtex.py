
import os
import requests
import anndata
import pandas as pd

GTEX_URL = 'https://storage.googleapis.com/gtex_analysis_v9/snrna_seq_data/GTEx_8_tissues_snRNAseq_immune_atlas_071421.public_obs.h5ad'
GTEX_DATA_PATH = 'gtex_data.h5ad'

def download_data(url):
    response = requests.get(url)
    with open("gtex_data.h5ad", mode="wb") as file:
        file.write(response.content)

def get_sample_info(file):
    adata = anndata.read_h5ad(file)
    return adata.obs[['Tissue Site Detail','Sample ID short']].drop_duplicates().reset_index(drop=True)


if __name__ == "__main__":
    if not os.path.isfile(GTEX_DATA_PATH):
        download_data(GTEX_URL)
    gtex_info = pd.DataFrame()
    gtex_info[['tissue','sample_id']] = get_sample_info(GTEX_DATA_PATH)
    gtex_info.to_csv('gtex.csv')
    os.remove(GTEX_DATA_PATH)