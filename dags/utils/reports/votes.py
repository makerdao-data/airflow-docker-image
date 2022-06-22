import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe


def populate_vote_sheet(sheet: gspread.spreadsheet.Spreadsheet) -> None:
    """
    Function to populate 'Vote data export' gsheet
    """
    
    ## Executives ##
    
    # Fetch executives data
    url = "https://vote.makerdao.com/api/executive?network=mainnet"
    executives = requests.get(url).json()
    # Instantiate worksheet object and fetch exec titles
    exec_ws = sheet.worksheet("test_execs")
    exec_ws_titles = exec_ws.col_values(1)[1:]
    # Isolate needed fields from list of executives
    pre_df = []
    for exec in executives:
        val = { k:v for k, v in exec.items() if k in ('title', 'proposalBlurb', 'key', 'address', 'date', 'active', 'proposalLink') }
        pre_df.append(dict(list(val.items()) + list(exec['spellData'].items())))
    # Store as dataframe and create id column
    exec_new_df = pd.DataFrame.from_records(pre_df)[
        ['title', 
        'proposalBlurb', 
        'key',
        'address',
        'date',
        'active',
        'proposalLink',
        'hasBeenCast', 
        'hasBeenScheduled', 
        'expiration', 
        'mkrSupport', 
        'executiveHash', 
        'officeHours',
        'dateExecuted',
        'datePassed',
        'eta',
        'nextCastTime']
    ].sort_index(ascending=False).reset_index(drop=True)
    try:
        # Identify which values are to be uploaded with iterative title indexing
        start_idx = [i in exec_ws_titles for i in exec_new_df.title.values].index(False)
        exec_upload = exec_new_df.loc[start_idx:]
    except ValueError:
        # Ideally would just be excluded from the below loop via additional logic
        exec_upload = pd.DataFrame()
        
    ## Polls ##
    
    # Fetch poll data
    polls_ws = sheet.worksheet("test_polls")
    url = "https://governance-portal-v2.vercel.app/api/polling/all-polls"
    res = requests.get(url).json()
    # Store as dataframe and sort by id
    polls = pd.DataFrame(res['polls'])    
    polls = polls.sort_values(by='pollId').drop(columns='cursor')
    # Get list of stored values
    stored_polls = polls_ws.col_values(2)
    # Select values to upload by filtering with id  
    polls_upload = polls[polls.pollId > int(stored_polls[-1])]

    ## Upload ##
    
    # Iterative uploading
    for upload in [(polls_ws, polls_upload, (len(stored_polls) + 1)),
                    (exec_ws, exec_upload, (len(exec_ws_titles) + 2))]: 
        if upload[1].empty:
            continue
        # If there are values to upload, upload them
        else:
            set_with_dataframe(upload[0], upload[1], row=upload[2], include_column_header=False)
    
    return