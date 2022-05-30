import json
import sys

sys.path.append('/opt/airflow/')

import gspread
from airflow.exceptions import AirflowFailException

with open('/opt/airflow/config/serv_account.json', 'r') as fp:
    service_account_info = json.load(fp)

try:
    gclient = gspread.service_account_from_dict(service_account_info)
except:
    raise AirflowFailException("Could not connect to gspread.")