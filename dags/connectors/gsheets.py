import json
import sys

sys.path.append('/opt/airflow/')

import gspread
from airflow.exceptions import AirflowFailException
from config import SERV_ACCOUNT

with open(SERV_ACCOUNT, 'r') as serv_account_creds:
    service_account_info = json.load(serv_account_creds)

try:
    gclient = gspread.service_account_from_dict(service_account_info)
except:
    raise AirflowFailException("Could not connect to gspread.")