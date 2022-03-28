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

    delegates = dict()
    for vote_delegate, start_date, type, name in sf.execute(f"""
            SELECT vote_delegate, start_date, type, name
            FROM delegates.public.delegates;
        """).fetchall():

        delegates[vote_delegate] = dict(
            start_date=start_date,
            type=type,
            name=name
        )

    records = list()

    for eod, weight, vote_delegate in sf.execute(f"""
            SELECT to_varchar(p.eod::date) as eod, p.balance as weight, p.vote_delegate
            FROM delegates.public.power p
            WHERE eod > '{response['values'][-1][0]}'
            ORDER BY eod;
        """).fetchall():

        type = None
        name = None

        if vote_delegate in delegates:
            if delegates[vote_delegate]['start_date']:
                if delegates[vote_delegate]['start_date'].__str__()[:10] <= eod:
                    type = delegates[vote_delegate]['type']
                    name = delegates[vote_delegate]['name']
            else:
                type = delegates[vote_delegate]['type']
                name = vote_delegate
        
        records.append([
            eod,
            type,
            name,
            weight
        ])
    
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