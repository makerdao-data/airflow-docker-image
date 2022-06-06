import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from gspread_dataframe import set_with_dataframe
from gspread.spreadsheet import Worksheet
from typing import Dict
from datetime import timedelta

def fetch_data(engine) -> pd.DataFrame:
    """
    Function to fetch fee data
    """

    # Fetch fee data
    work_df = pd.read_sql("select DAY, ILK, FEES from MAKER.HISTORY.VAULTS", engine)
    
    # Convert DAY column from string to datetime
    work_df['DAY'] = pd.to_datetime(work_df['DAY'])
    
    # Group by ilk
    grouped_df = work_df.groupby('ILK')

    return grouped_df


def filter_data(grouped_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Function to filter through data and create fee reports
    """

    # Create result storage
    res = {}

    # Iterate through dataframes
    for operation in [('weekly', 'W-MON', lambda x: x), ('monthly', 'M', np.cumsum)]:
        
        # Resample/reorg datafrane
        resampled_df = grouped_df.resample(operation[1], on='DAY').sum()
        resampled_df.reset_index(inplace=True)
        resampled_df.sort_values(by='DAY', inplace=True)
        
        # Apply operations for each unique ILK
        for ilk in resampled_df['ILK'].unique():
            
            # Process fees
            processed_series = round(operation[2](resampled_df[resampled_df['ILK'] == ilk]['FEES']), 2) 
            resampled_df.loc[resampled_df.ILK == ilk, 'FEES'] = processed_series
            
            # If operation is for weekly data...
            if operation[0] == 'weekly':
                
                # Stores of week data
                week_start = []
                week_end = []
                week_num = []
                
                # Obtain end of week, start of week and week number for days
                for date_obj in resampled_df.loc[resampled_df.ILK == ilk, 'DAY']:
                    sow = date_obj - timedelta(days=date_obj.weekday())
                    eow = sow + timedelta(days=6)
                    week_start.append(sow.date())
                    week_end.append(eow.date())
                    week_num.append(date_obj.isocalendar()[1])

                # Apply new columns
                resampled_df.loc[resampled_df.ILK == ilk, 'END_OF_WEEK'] = week_end
                resampled_df.loc[resampled_df.ILK == ilk, 'START_OF_WEEK'] = week_start
                resampled_df.loc[resampled_df.ILK == ilk, 'WEEK_NUM'] = week_num

        # Drop DAY column in weekly gen
        if operation[0] == 'weekly': resampled_df.drop(columns='DAY', inplace=True)
        else: resampled_df.DAY = resampled_df.DAY.dt.date
        
        # Store in result 
        res[operation[0]] = resampled_df
    
    return res


def upload_data(dfs: Dict[str, pd.DataFrame], sheet: Worksheet) -> None:
    """
    Upload vote data. Could be abstracted into one loop. Will do later.
    """
    
    wk = dfs['weekly']
    mo = dfs['monthly']
    
    # Select sheet
    weekly = sheet.worksheet("Weekly Fees Paid")
    # Obtain END_OF_WEEK column
    all_weeks = weekly.col_values(3)
    # Get last date in column
    last_week = datetime.strptime(all_weeks[-1], '%Y-%m-%d')
    # Identiy insertion index
    idx = len(all_weeks) + 1 
    # Upload conditionals
    cond = (wk.END_OF_WEEK > last_week.date()) & (wk.END_OF_WEEK < date.today())
    # If dataframe w/ conditional applied is not empty
    if not wk.loc[cond].empty:
        # Upload weekly update
        set_with_dataframe(weekly, wk.loc[cond], row=idx, include_column_header=False)
    else:
        print("No update needed.")
    
    # Select sheet
    monthly = sheet.worksheet("Monthly Cumulative Fees")
    # Obtain DAY column
    all_months = monthly.col_values(2)
    # Get last date in column
    last_month = datetime.strptime(all_months[-1], '%Y-%m-%d')
    # Identify insertion index
    idx = len(all_months) + 1 
    # Upload conditionals
    cond = (mo.DAY > last_month.date()) & (mo.DAY < date.today())
    # If dataframe w/ conditional applied is not empty
    if not mo.loc[cond].empty:
        # Upload monthly update
        set_with_dataframe(monthly, mo.loc[cond], row=idx, include_column_header=False)
    else:
        print("No update needed.")
        
    return

def upload_wrapper(engine, gsheet) -> None:
    """
    Wrapper for performance FEE upload functions
    """

    fetched = fetch_data(engine)
    filtered = filter_data(fetched)
    _ = upload_data(filtered, gsheet)
    
    return