import httpx
import os
from dotenv import load_dotenv
from utils.elastic import bulk_insert, get_elastic_client 
import pandas as pd
import datetime

load_dotenv()
es = get_elastic_client()

def get_stops():
    with httpx.Client() as client:
        response = client.get(os.getenv('API_URL_STOPS'))
        response = response.json()
        for line in response:
            line['DATE'] = datetime.datetime.now().isoformat()
        df = pd.DataFrame(response)
        return df
