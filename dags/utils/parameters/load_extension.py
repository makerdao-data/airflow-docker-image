import sys
sys.path.append('/opt/airflow/')
import math
import datetime
import snowflake
import pandas as pd
import numpy as np
import web3
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage


def new_flopper_params(engine: snowflake.connector.connection.SnowflakeConnection,
                       chain: web3.main.Web3, setup: dict) -> pd.DataFrame:    
    """
    Function to fetch new Flopper parameters:
        - tau
    Parameters:
        - engine
            - snowflake.connector.connection.SnowflakeConnection
        - chain
            - web3.main.Web3
    """

    # Fetch data
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'FLOPPER.tau' as parameter
                    from edw_share.raw.storage_diffs 
                        where LOCATION = '6' 
                        and contract = '0xa41b6ef151e06da0e34b009b86e828308986736d'
                        and block > {setup['start_block']} and block <= {setup['end_block']}
                        """
    result = pd.read_sql(query, engine)
    result.replace('0x', '0x0', inplace=True)

    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(result.at[i, 'PREV_VALUE'][:8], 16)
        result.at[i, 'CURR_VALUE'] = int(result.at[i, 'CURR_VALUE'][:8], 16)
        src = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
        src = src.lower() if src else src
        result.at[i, 'SOURCE'] = src

    # Return DataFrame
    return result


def new_flapper_params(engine: snowflake.connector.connection.SnowflakeConnection,
                       chain: web3.main.Web3, setup: dict) -> pd.DataFrame:
    """
    Function to fetch new Flapper parameters:
        - tau
    """

    # Fetch data
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'FLAPPER.tau' as parameter
                from edw_share.raw.storage_diffs 
                    where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' 
                    and location = '5'
                    and block > {setup['start_block']} and block <= {setup['end_block']}"""
    result = pd.read_sql(query, engine)
    result.replace('0x', '0x0', inplace=True)

    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(str(result.at[i, 'PREV_VALUE'])[:8], 16)
        result.at[i, 'CURR_VALUE'] = int(str(result.at[i, 'CURR_VALUE'])[:8], 16)
        src = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
        src = src.lower() if src else src
        result.at[i, 'SOURCE'] = src

    # Return DataFrame
    return result


def new_esm_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3, setup: dict) -> pd.DataFrame:  
    """
    Function to fetch new ESM parameters:
        - min
    """

    # Fetch last updated value
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'ESM.min' as parameter
                from edw_share.raw.storage_diffs 
                    where contract = '0x09e05ff6142f2f9de8b6b65855a1d56b6cfe4c58' 
                    and location = '3'
                    and block > {setup['start_block']} and block <= {setup['end_block']}"""
    result = pd.read_sql(query, engine)
    result.replace('0x', '0x0', inplace=True)

    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(str(result.at[i, 'PREV_VALUE'])[:8], 16)
        result.at[i, 'CURR_VALUE'] = int(str(result.at[i, 'CURR_VALUE'])[:8], 16)
        src = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
        src = src.lower() if src else src
        result.at[i, 'SOURCE'] = src

    # Return DataFrame
    return result


def new_psm_params(engine: snowflake.connector.connection.SnowflakeConnection, 
                   chain: web3.main.Web3, setup: dict) -> pd.DataFrame:
    """
    Function to fetch new PSM parameters:
        - tin
        - tout
    Will compress function.
    """

    results = []
    # Iterate through contracts
    for contract in [('0x961ae24a1ceba861d1fdf723794f6024dc5485cf', 'PSM-USDP-A'), 
                     ('0x89b78cfa322f6c5de0abceecab66aee45393cc5a', 'PSM-USDC-A'),
                     ('0x204659b2fd2ad5723975c362ce2230fba11d3900', 'PSM-GUSD-A')]:

        # Fetch parameters
        query = f"""select block, timestamp, tx_hash, prev_value, curr_value, location
            from edw_share.raw.storage_diffs 
                where contract = '{contract[0]}' 
                and location in ('1', '2')
                and block > {setup['start_block']} and block <= {setup['end_block']}"""
        result = pd.read_sql(query, engine)
        result.replace('0x', '0x0', inplace=True)

        # Iterate through rows, format and populate values
        for i in range(len(result)):
            result.at[i, 'PREV_VALUE'] = (int(result.at[i, 'PREV_VALUE'], 16) / 10**18)
            result.at[i, 'CURR_VALUE'] = (int(result.at[i, 'CURR_VALUE'], 16) / 10**18)
            src = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
            src = src.lower() if src else src
            result.at[i, 'SOURCE'] = src

        # Add parameter column.
        result['ILK'] = contract[1]

        # Format
        result.LOCATION.replace(to_replace='1', value='PSM.tin',  inplace=True)
        result.LOCATION.replace(to_replace='2', value='PSM.tout',  inplace=True)
        result.rename(columns={'LOCATION':'PARAMETER'}, inplace=True)

        # Append to result store
        results.append(result)

    return pd.concat([results[0], results[1], results[2]], axis=0)    


def new_dspause_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3, setup: dict) -> pd.DataFrame:
    """
    Function to fetch new DSPause parameters:
        - pause
    """

    # Fetch last updated value
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'DSPAUSE.delay' as parameter
                from edw_share.raw.storage_diffs 
                    where contract = '0xbe286431454714f511008713973d3b053a2d38f3' 
                    and location = '4'
                    and block > {setup['start_block']} and block <= {setup['end_block']}"""
    result = pd.read_sql(query, engine)
    result.replace('0x', '0x0', inplace=True)

    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(str(result.at[i, 'PREV_VALUE'])[:8], 16)
        result.at[i, 'CURR_VALUE'] = int(str(result.at[i, 'CURR_VALUE'])[:8], 16)
        src = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
        src = src.lower() if src else src
        result.at[i, 'SOURCE'] = src

    # Return DataFrame
    return result


def new_end_params(engine: snowflake.connector.connection.SnowflakeConnection,
                     chain: web3.main.Web3, setup: dict) -> pd.DataFrame:
    """
    Function to fetch new END parameters:
        - wait
    """

    # Fetch last updated value
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'GSM.pause' as parameter
                from edw_share.raw.storage_diffs 
                    where contract = '0xbb856d1742fd182a90239d7ae85706c2fe4e5922' 
                    and location = '9'
                    and block > {setup['start_block']} and block <= {setup['end_block']}"""
    result = pd.read_sql(query, engine)
    result.replace('0x', '0x0', inplace=True)

    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(str(result.at[i, 'PREV_VALUE'])[:8], 16)
        result.at[i, 'CURR_VALUE'] = int(str(result.at[i, 'CURR_VALUE'])[:8], 16)
        src = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
        src = src.lower() if src else src
        result.at[i, 'SOURCE'] = src

    # Return DataFrame
    return result


def get_new_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3, setup: dict) -> pd.DataFrame:
    """
    Construct dataframe of parameter additions.
    """

    # Fetch results
    newesm = new_esm_params(engine, chain, setup)
    newpsm = new_psm_params(engine, chain, setup)
    newflop = new_flopper_params(engine, chain, setup)
    newflap = new_flapper_params(engine, chain, setup)
    newgsm = new_dspause_params(engine, chain, setup)
    newpause = new_end_params(engine, chain, setup)
    
    # Formatting assurance
    concatenated = pd.concat([newesm, newpsm, newflop, newflap, newgsm, newpause], axis=0)
    concatenated.fillna(value=np.nan, inplace=True)
    concatenated.PREV_VALUE = [float(i) if not math.isnan(i) else None for i in concatenated.PREV_VALUE]
    concatenated.CURR_VALUE = [float(i) if not math.isnan(i) else None for i in concatenated.CURR_VALUE]
    concatenated.BLOCK = [int(i) for i in concatenated.BLOCK]
    # Why fillna just to replace with None? So the math.isnan comprehension covers all null values without erroring out. Quick fix.
    concatenated.replace({np.nan: None}, inplace=True) 
    concatenated = concatenated.reset_index(drop=True)
    
    # Removing all values with same pre & post values.
    for i in range(len(concatenated)):
        try:      
            if int(concatenated.at[i, 'PREV_VALUE']) == int(concatenated.at[i, 'CURR_VALUE']):
                if int(concatenated.at[i, 'CURR_VALUE']) != 0:
                    concatenated.drop(i, inplace=True)
        except:
            continue
        
    # Adding source column if nonexistent
    if 'SOURCE' not in concatenated: concatenated['SOURCE'] = None;

    return concatenated[['BLOCK','TIMESTAMP','TX_HASH','SOURCE','PARAMETER','ILK','PREV_VALUE','CURR_VALUE']]


def upload_new_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3, **setup) -> pd.DataFrame:
    """
    Generate and upload dataframe of newly obtained parameters.
    """
    
    result = get_new_params(engine, chain, setup)
    pattern = _write_to_stage(engine.cursor(), list(result.to_numpy()), f"MAKER.PUBLIC.PARAMETERS_STORAGE") 
    _write_to_table(engine.cursor(), f"MAKER.PUBLIC.PARAMETERS_STORAGE",f"MAKER.PUBLIC.PARAMETERS", pattern)
    _clear_stage(engine.cursor(), f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)
    return