import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe


def populate_vote_sheet(sheet: gspread.spreadsheet.Spreadsheet) -> None:
    """
    Function to populate 'Vote data export' gsheet
    """
    
    ## Executives ##
    
    # # Fetch executives data
    # url = "https://vote.makerdao.com/api/executive?network=mainnet"
    # r = requests.get(url)
    # executives = r.json()
    
    # # Isolate needed fields from list of executives
    # pre_df = []
    # for exec in executives:
    #     val = { k:v for k, v in exec.items() if k in ('title', 'proposalBlurb', 'key', 'address', 'date', 'active', 'proposalLink') }
    #     pre_df.append(dict(list(val.items()) + list(exec['spellData'].items())))
    
    # # Store as dataframe and create id column
    # execs = pd.DataFrame.from_records(pre_df)[
    #     ['title', 
    #     'proposalBlurb', 
    #     'key',
    #     'address',
    #     'date',
    #     'active',
    #     'proposalLink',
    #     'hasBeenCast', 
    #     'hasBeenScheduled', 
    #     'expiration', 
    #     'mkrSupport', 
    #     'executiveHash', 
    #     'officeHours',
    #     'dateExecuted',
    #     'datePassed',
    #     'eta',
    #     'nextCastTime']
    # ].sort_index(ascending=False).reset_index(drop=True)
    # execs = execs.reset_index().rename(columns={'index':'execId'})
        
    ## Polls ##
    
    # Fetch poll data
    polls_ws = sheet.worksheet("polls")
    url = "https://governance-portal-v2.vercel.app/api/polling/all-polls"
    r = requests.get(url)
    res = r.json()
    
    # Store as dataframe and sort by id
    polls = pd.DataFrame(res['polls'])    
    polls.sort_values(by='pollId', inplace=True)
    
    ## Upload ##
    
    # Iterative uploading
    for upload in [(2, 'pollId', sheet.worksheet("polls"), polls)]:
        # (1, 'execId', sheet.worksheet("executives"), execs)

        # Get list of stored values
        stored_vals = upload[2].col_values(upload[0])
        
        # Select values to upload by filtering with id  
        upload_vals = upload[3][upload[3][upload[1]] > int(stored_vals[-1])]
        
        # If there are no values to upload, skip iteration
        if upload_vals.empty:
            continue
        # If there are values to upload, upload them
        else:
            set_with_dataframe(upload[2], upload_vals, row=(len(stored_vals) + 1), include_column_header=False)
            
    return