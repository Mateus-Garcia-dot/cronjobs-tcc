import httpx
from dotenv import load_dotenv
from utils.elastic import get_elastic_client, bulk_insert
import pandas as pd
import datetime
import os

load_dotenv()
es = get_elastic_client()

def get_lines():
    with httpx.Client() as client:
        response = client.get(os.getenv('API_URL_LINES'))
        response = response.json()
        for line in response:
            line['DATE'] = datetime.datetime.now().isoformat()
        df = pd.DataFrame(response)
        return df

def create_index(index_name):
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "COD": {"type": "keyword"},
                "NOME": {"type": "text"},
                "SOMENTE_CARTAO": {"type": "keyword"},
                "CATEGORIA_SERVICO": {"type": "keyword"},
                "NOME_COR": {"type": "keyword"},
                "DATE": {"type": "date"}
            }
        }
    }
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_settings)

    
index_name = 'linha'
create_index(index_name)
bulk_insert(es, get_lines(), index_name)