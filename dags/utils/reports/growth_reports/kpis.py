"""
Automating Growth KPIs google sheets report.
"""
import calendar
import sys
import requests
from datetime import date, datetime
from typing import List, Tuple

import pandas as pd
from gspread.client import Client
from gspread.spreadsheet import Spreadsheet
from gspread_dataframe import set_with_dataframe
from snowflake.connector.cursor import SnowflakeCursor
from airflow.exceptions import AirflowFailException

def _collect_data(sf: SnowflakeCursor) -> Tuple[pd.DataFrame]:
    """
    Collecting data required for analyses
    """
    # Fetching 1/2 of needed data then renaming columns and stripping datetime
    today = date.today()
    try:
        hist_vaults = pd.DataFrame(
            sf.execute(
                f"""SELECT PRINCIPAL, AVAILABLE_DEBT, VAULT, ILK, OSM_PRICE, COLLATERAL, LAST_TIME AS DATE FROM MCD.PUBLIC.CURRENT_VAULTS WHERE LAST_TIME > '{date(today.year, today.month, 1).strftime("%m/%d/%Y")}'"""
            ).fetchall())
    except:
        raise AirflowFailException("Growth Report data sourcing query failed.")

    # Renaming columns and stripping time from dates
    hist_vaults.rename(columns={
        0: 'PRINCIPAL',
        1: 'AVAILABLE_DEBT',
        2: 'VAULT',
        3: 'ILK',
        4: 'OSM_PRICE',
        5: 'COLLATERAL',
        6: 'DATE'
    }, inplace=True)
    hist_vaults['DATE'] = hist_vaults.loc[:, 'DATE'].dt.date

    # Fetching 2/2 of needed data then processing json
    tokens = ('WETH', 'WBTC')
    bret = []
    for token in tokens:
        try:
            resp = requests.get(f'https://api.blockanalitica.com/api/defi/locked/{token}/?format=json')
        except:
            raise AirflowFailException(f"Failed to collect blockanalitica data for token: {token}")
        resp_json = resp.json()
        df = pd.DataFrame(resp_json['rows'], columns=resp_json['column_names']).set_index('Timestamp')
        bret.append(df)
    hist_collat = pd.concat([bret[0], bret[1]], axis=1).reset_index()
    hist_collat['Timestamp'] = hist_collat['Timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))

    return (hist_vaults, hist_collat)


def _conduct_analyses(vaults_df: pd.DataFrame) -> Tuple[pd.DataFrame]:
    """
    Conduct growth report analyses
    """
    # Analysis 1 (Dai distribution & available debt)
    df = vaults_df[vaults_df['PRINCIPAL'] > 10]
    df.loc[:, 'AVAILABLE_DEBT_PERCENT'] = df['AVAILABLE_DEBT'] / (
        df['PRINCIPAL'] + df['AVAILABLE_DEBT'])
    bins = [10, 10000, 100000, 1000000, 10000000, 10000000000000]
    labels = ['< 10k', '10k-100k', '100k-1M', '1M-10M', '> 10M']
    df.loc[:, 'BINS'] = pd.cut(df['PRINCIPAL'],
                               bins=bins,
                               labels=labels,
                               include_lowest=True)
    df_1 = (df.groupby(by=['DATE', 'ILK', 'BINS']).agg({
        'PRINCIPAL':
        'sum',
        'AVAILABLE_DEBT_PERCENT':
        'mean',
        'VAULT':
        'nunique'
    }).reset_index())

    # Analysis 2 (Volume of collateral locked)
    df = vaults_df
    df.loc[:, 'COLLATERAL_DAI'] = df['COLLATERAL'] * df['OSM_PRICE']
    df = df[df['COLLATERAL_DAI'] > 10]
    df.loc[:, 'BINS'] = pd.cut(df['COLLATERAL_DAI'],
                               bins=bins,
                               labels=labels,
                               include_lowest=True)
    df_2 = (df.groupby(by=['DATE', 'ILK', 'BINS']).agg({
        'PRINCIPAL': 'sum',
        'COLLATERAL_DAI': 'sum',
        'VAULT': 'nunique'
    }).reset_index())

    return (df_1, df_2)


def _upload(dfs: Tuple[pd.DataFrame], gsheet: Spreadsheet) -> None:
    """
    Upload analyses to google sheets
    """

    uploads = (
        [dfs[0], gsheet.worksheet("Overall collateral market")],
        (dfs[1], gsheet.worksheet("Dai distribution & available debt")),
        (dfs[2], gsheet.worksheet("Volume of collateral locked")),
    )

    # Iterate through worksheets
    for upload in uploads:

        # Get all dates from spreadsheet, remove empty rows
        dates = upload[1].col_values(1)
        if ' ' in dates:
            dates.remove(' ')

        # Get last date, process dataframe if needed
        if '-' in dates[-1]:
            last_date = dates[-1].split(' ')[0]
            ymd_last_date = list(map(int, last_date.split('-')))
            idx = (dates.index(dates[-1]) + 1)
        else:
            last_date = datetime.strptime(dates[-1], '%m/%d/%Y')
            upload[0] = upload[0][upload[0]['Timestamp'] > last_date]
            upload[0]['Timestamp'] = upload[0]['Timestamp'].apply(lambda x: x.strftime('%m/%d/%Y'))
            ymd_last_date = [last_date.year, last_date.month, last_date.day]
            idx = (len(dates) + 1)

        # Get current date
        today = date.today()

        # Check sheet for update location, or if current update is to be skipped
        if today.month == ymd_last_date[1]:
            if today.day > ymd_last_date[2]:
                # Update insertion
                try:
                    set_with_dataframe(upload[1],
                                       upload[0],
                                       row=idx,
                                       include_column_header=False)
                except:
                    raise AirflowFailException(
                        f"Growth report update insertion for sheet '{upload[1].title}' failed."
                    )
                print(f"Growth report for sheet '{upload[1].title}' updated.")
            else:
                print(
                    f"Growth report for sheet '{upload[1].title}' requires no update."
                )
        else:
            # Update insertion
            try:
                set_with_dataframe(upload[1],
                                   upload[0],
                                   row=(len(dates) + 1),
                                   include_column_header=False)
            except:
                raise AirflowFailException(
                    f"Growth report update insertion for sheet '{upload[1].title}' failed."
                )
            print(f"Growth report for sheet '{upload[1].title}' updated.")

    return


def growth_report_updater(sf: SnowflakeCursor, gclient: Client) -> None:
    vaults_df, collats_df  = _collect_data(sf)
    dist_debt_df, lock_coll_df = _conduct_analyses(vaults_df)

    _upload(
        (collats_df, dist_debt_df, lock_coll_df),
        gclient.open("Growth KPIs #1"),
    )

    return
