import json
import sys
import os
import gspread
from airflow.exceptions import AirflowFailException


with open(os.environ.get('SERV_ACCOUNT'), 'r') as fp:
        service_account = json.load(fp)

try:
    gclient = gspread.service_account_from_dict(service_account)
except:
    raise AirflowFailException("Could not connect to gspread.")
