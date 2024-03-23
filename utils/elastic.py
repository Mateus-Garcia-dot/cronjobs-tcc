from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dotenv import load_dotenv
import os

load_dotenv()

def get_elastic_client():
    client = Elasticsearch(
        os.getenv('ELASTIC_URL'),
        verify_certs=False,
        basic_auth=("elastic", os.getenv('ELASTIC_PASSWORD'))
    )
    return client



def bulk_insert(es, df, index_name):
    def doc_generator(df):
        for index, row in df.iterrows():
            yield {
                "_index": index_name,
                "_source": row.to_dict(),
            }

    bulk(es, doc_generator(df))