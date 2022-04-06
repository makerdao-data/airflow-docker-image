import json

import gspread
from airflow.exceptions import AirflowFailException
from config import SERV_ACCOUNT

sys.path.append('/opt/airflow/')

try:
    gclient = gspread.service_account_from_dict(json.loads(SERV_ACCOUNT))
except:
    raise AirflowFailException("Could not connect to gspread.")
