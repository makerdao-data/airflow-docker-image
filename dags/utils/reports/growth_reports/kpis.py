"""
Automating Growth KPIs google sheets report.
"""

import calendar
import sys
from datetime import date
from typing import Tuple

import pandas as pd
from gspread.client import Client
from gspread.spreadsheet import Spreadsheet
from gspread_dataframe import set_with_dataframe
from snowflake.connector.cursor import SnowflakeCursor


def _conduct_analyses(sf: SnowflakeCursor) -> Tuple[pd.DataFrame]:
    """
    Conduct growth report analyses
    """

    # Fetching data then renaming columns and stripping datetime
    today = date.today()
    try:
        hist_vaults = pd.DataFrame(
            sf.execute(
                f"""SELECT PRINCIPAL, AVAILABLE_DEBT, VAULT, ILK, OSM_PRICE, COLLATERAL, LAST_TIME AS DATE FROM MCD.PUBLIC.CURRENT_VAULTS WHERE LAST_TIME > '{date(today.year, today.month, 1).strftime("%m/%d/%Y")}'"""
            ).fetchall())
    except:
        raise AirflowFailException("Growth Report data sourcing query failed.")
    renamed_columns = {}
    for key in list(hist_vaults.keys()):
        renamed_columns[key] = sf.description[key].name
    hist_vaults.rename(columns=renamed_columns, inplace=True)
    hist_vaults['DATE'] = hist_vaults.loc[:, 'DATE'].dt.date

    # Analysis 1 (Dai distribution & available debt)
    df = hist_vaults[hist_vaults['PRINCIPAL'] > 10]
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
    df = hist_vaults
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
        (dfs[0], gsheet.worksheet("Dai distribution & available debt")),
        (dfs[1], gsheet.worksheet("Volume of collateral locked")),
    )

    for upload in uploads:
        # Get all dates from spreadsheet, remove empty rows
        dates = upload[1].col_values(1)
        if ' ' in dates:
            dates.remove(' ')
        # Get last and current date
        last_date = dates[-1].split(' ')[0]
        today = date.today()
        ymd_last_date = list(map(int, last_date.split('-')))

        # Check sheet for update location, or if current update is to be skipped
        if today.month == ymd_last_date[1]:
            if today.day > ymd_last_date[2]:
                # Update insertion
                try:
                    set_with_dataframe(upload[1],
                                       upload[0],
                                       row=(dates.index(dates[-1]) + 1),
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
    _upload(
        _conduct_analyses(sf),
        gclient.open("Growth KPIs #1"),
    )

    return
