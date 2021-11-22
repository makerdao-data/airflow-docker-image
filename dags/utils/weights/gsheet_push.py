import json
from googleapiclient import discovery
from google.oauth2 import service_account
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf


def _gsheet_push():

    with open('/opt/airflow/config/quickstart-1587209627813-93a59c68f68e.json', 'r') as fp:
        service_account_info = json.load(fp)

    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    service = discovery.build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()

    SPREADSHEET_ID = '1KGxO-A6MONftckKMhohiY4WspsC1r-gTuu3mvKkjcPk'
    SHEET_NAME = 'result'

    # append new row on empty row from Row 2 onwards
    range_notation = f"{SHEET_NAME}!A2:D"

    request = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_notation)
    response = request.execute()

    records = sf.execute(f"""
        select to_varchar(p.eod::date) as eod, d.type as type, case when d.name is null then d.vote_delegate else d.name end as name, p.balance as weight
        from delegates.public.power p, delegates.public.delegates d
        where p.vote_delegate = d.vote_delegate and
            eod > '{response['values'][-1][0]}'
        order by eod;
    """
    ).fetchall()

    range_notation = f"{SHEET_NAME}!A2"

    body = {
        'values': records
    }

    result = sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=range_notation,
        body=body,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS"
    ).execute()

    return