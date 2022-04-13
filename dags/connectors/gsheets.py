import json
import sys

sys.path.append('/opt/airflow/')

import gspread
from airflow.exceptions import AirflowFailException
from config import SERV_ACCOUNT


try:
    gclient = gspread.service_account_from_dict(json.loads(SERV_ACCOUNT))
except:
    raise AirflowFailException("Could not connect to gspread.")