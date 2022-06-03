import pandas as pd
import numpy as np
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
        resampled_df.sort_values(by='ILK', inplace=True)
        
        # Apply operations for each unique ILK
        for ilk in resampled_df['ILK'].unique():
            
            # Cumulatively sum fees
            cumsum_series = round(operation[2](resampled_df[resampled_df['ILK'] == ilk]['FEES']), 2) 
            resampled_df.loc[resampled_df.ILK == ilk, 'FEES'] = cumsum_series
            
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
        
        # Store in result 
        res[operation[0]] = resampled_df
    
    return res