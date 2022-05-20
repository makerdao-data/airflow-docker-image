import web3
import math
import datetime
import snowflake
import pandas as pd
import numpy as np
from dags.connectors.sf import _write_to_stage, _write_to_table

def new_flopper_params(engine: snowflake.connector.connection.SnowflakeConnection,
                       chain: web3.main.Web3) -> pd.DataFrame:    
    """
    Function to fetch new Flopper parameters:
        - tau
    
    Parameters:
        - engine
            - snowflake.connector.connection.SnowflakeConnection
        - chain
            - web3.main.Web3
    """
    
    # Identify which blocks to parse
    last_block = engine.cursor().execute("""select max(block) from maker.public.parameters where parameter ='FLOPPER.tau'""").fetchone()[0]
    if not last_block:
        last_block = 0

    # Fetch data
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'FLOPPER.tau' as parameter
                    from edw_share.raw.storage_diffs 
                        where LOCATION = '6' 
                        and contract = '0xa41b6ef151e06da0e34b009b86e828308986736d'
                        and block > {last_block}
                        """
    result = pd.read_sql(query, engine)
    
    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(result.at[i, 'PREV_VALUE'][:8], 16)
        result.at[i, 'CURR_VALUE'] = int(result.at[i, 'CURR_VALUE'][:8], 16)
        result.at[i, 'SOURCE'] = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
    
    # Return DataFrame
    return result


def new_flapper_params(engine: snowflake.connector.connection.SnowflakeConnection,
                       chain: web3.main.Web3) -> pd.DataFrame:
    """
    Function to fetch new Flapper parameters:
        - tau
    """
    
    # Identify which blocks to parse
    last_block = engine.cursor().execute("""select max(block) from maker.public.parameters where parameter ='FLOPPER.tau'""").fetchone()[0]
    if not last_block:
        last_block = 0

    # Fetch data
    query = f"""select block, timestamp, tx_hash, prev_value, curr_value, 'FLAPPER.tau' as parameter
                from edw_share.raw.storage_diffs 
                    where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' 
                    and location = '5'
                    and block > {last_block}"""
    result = pd.read_sql(query, engine)
    
    # Iterate through columns, format and populate values
    for i in range(len(result)):
        result.at[i, 'PREV_VALUE'] = int(str(result.at[i, 'PREV_VALUE'])[:8], 16)
        result.at[i, 'CURR_VALUE'] = int(str(result.at[i, 'CURR_VALUE'])[:8], 16)
        result.at[i, 'SOURCE'] = chain.eth.get_transaction(result.at[i, 'TX_HASH'])['to']
    
    # Return DataFrame
    return result


def new_esm_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3) -> pd.DataFrame:  
    """
    Function to fetch new ESM parameters:
        - min
    """
    
    # Fetch last updated value
    last_val = engine.cursor().execute("""select max(block), to_value from maker.public.parameters where parameter = 'ESM.min' group by to_value""").fetchone()
    
    # Fetch current value
    result = int(
        int(
            chain.eth.getStorageAt(
                account=Web3.toChecksumAddress(
                    '0x09e05fF6142F2f9de8B6B65855A1d56B6cfE4c58'
                ), 
                position=3
            ).hex(), 16
        ) * 10e-19
    )
    
    # If parameter not existent in dataset, create first entry
    if not last_val:
        block = chain.eth.get_transaction('0x814c35277b46ecbb148f280c7dfc5bc38bc5251c4530675e52c32a2cd7e3b3ce')['blockNumber']
        timestamp = pd.Timestamp(engine.cursor().execute(f"select TIMESTAMP from EDW_SHARE.RAW.BLOCKS where BLOCK = {block}").fetchone()[0])
        source = '0x09e05fF6142F2f9de8B6B65855A1d56B6cfE4c58'
        tx_hash = '0x814c35277b46ecbb148f280c7dfc5bc38bc5251c4530675e52c32a2cd7e3b3ce'
        prev_value = None
    # Do nothing if last updated value is equal to current. No update needed.
    elif last_val[1] == result:
        return None
    # If value updated, create entry with current block, timestamp and source. 
    # Note: this should be replaced with fields on the parameter-changing tx. 
    # Nothing in database yet to test this with, so defaulting to fields relating to moment of checking.
    # Source and Tx hash unable to be included due to the aforementioned. 
    # Return None if last updated value is equal to current. No update needed.
    else:
        block = chain.eth.blockNumber 
        timestamp = pd.Timestamp(datetime.fromtimestamp(chain.eth.getBlock(block).timestamp))
        source = None 
        tx_hash = None
        prev_value = last_val[1]
    
    # Construct dataframe
    df = pd.DataFrame()
    df.at[0, 'BLOCK'] = block
    df['TIMESTAMP'] = timestamp
    df['TX_HASH'] = tx_hash
    df['PREV_VALUE'] = prev_value
    df['CURR_VALUE'] = result
    df['PARAMETER'] = 'ESM.min'
    df['SOURCE'] = source

    return df


def new_psm_params(engine: snowflake.connector.connection.SnowflakeConnection, 
                   chain: web3.main.Web3) -> pd.DataFrame:
    """
    Function to fetch new PSM parameters:
        - tin
        - tau
        
    Will compress function.
    """
    results = []
    # Iterate through contracts
    for contract in [('0x961Ae24a1Ceba861D1FDf723794f6024Dc5485Cf','0x562bc8d3c306de58215fd01307eb29f399e57cbcf1e6ccf028ed4d75f09e2df2', 'PSM-USDP-A'), 
                     ('0x89B78CfA322F6C5dE0aBcEecab66Aee45393cC5A','0xbb54a3ae4109d0606be7b980db10e93915563dddd5fc00515cdc1e9757c3213a', 'PSM-USDC-A'),
                     ('0x204659B2Fd2aD5723975c362Ce2230Fba11d3900', '0x027a5efef89f9e0cf070278263d7db9ab9759fa8223d77c7b063384aac7b47b8', 'PSM-GUSD-A')]:
        # Create result df
        df = pd.DataFrame()
        
        # Fetch current and previously stored tin values
        tin = int(chain.eth.getStorageAt(account=Web3.toChecksumAddress(contract[0]), position=5).hex(), 16)
        last_tin = engine.cursor().execute(
            f"""select max(block), to_value from maker.public.parameters where parameter = 'PSM.tin' and ilk ='{contract[2]}' group by to_value"""
        ).fetchone()
        
        proc = True
        # If previous tin value does not exist, create entry with first
        if not last_tin:
            # Set variables for result dataframe
            tx_hash = contract[1]
            block = int(chain.eth.get_transaction(contract[1])['blockNumber'])
            source = contract[0]
            timestamp = pd.Timestamp(engine.cursor().execute(f"select TIMESTAMP from EDW_SHARE.RAW.BLOCKS where BLOCK = {block}").fetchone()[0])
            prev_value = None
            curr_value = tin
        # Do nothing if last updated value is equal to current. No update needed.
        elif last_tin[1] == tin:
            print('eq')
            proc = False
        # If value updated, create entry with current block, timestamp and source. 
        # Note: this should be replaced with fields on the parameter-changing tx. 
        # Nothing in database yet to test this with, so defaulting to fields relating to moment of checking.
        # Source and Tx hash unable to be included due to the aforementioned. 
        else:
            tx_hash = None
            block = chain.eth.blockNumber
            source = None 
            timestamp = pd.Timestamp(datetime.fromtimestamp(chain.eth.getBlock(block).timestamp))
            prev_value = last_tin[1]
            curr_value = tin
        
        if proc:
            # Populate df for tin
            df.at[0, 'PREV_VALUE'] = prev_value
            df.at[0, 'CURR_VALUE'] = curr_value
            df.at[0, 'PARAMETER'] = 'PSM.tin'
            df.at[0, 'TIMESTAMP'] = timestamp
            df.at[0, 'TX_HASH'] = tx_hash
            df.at[0, 'BLOCK'] = block
            df.at[0, 'SOURCE'] = source
            df.at[0, 'ILK'] = contract[2]
        
        # Fetch current and previously stored tout values
        tout = int(chain.eth.getStorageAt(account=Web3.toChecksumAddress(contract[0]), position=6).hex(), 16)
        last_tout = engine.cursor().execute(
            f"""select max(block), to_value from MAKER.PUBLIC.PARAMETERS where parameter = 'PSM.tout' group by to_value"""  
        ).fetchone()
        
        proc = True
        # If previous tin value does not exist, create entry with first
        if not last_tout:
            # Set variables for result dataframe
            tx_hash = contract[1]
            block = int(chain.eth.get_transaction(contract[1])['blockNumber'])
            source = contract[0]
            timestamp = pd.Timestamp(engine.cursor().execute(f"select TIMESTAMP from EDW_SHARE.RAW.BLOCKS where BLOCK = {block}").fetchone()[0])
            prev_value = None
            curr_value = tout
        # Do nothing if last updated value is equal to current. No update needed.
        elif tout == last_tout[1]:
            proc = False
        # If value updated, create entry with current block, timestamp and source. 
        # Note: this should be replaced with fields on the parameter-changing tx. 
        # Nothing in database yet to test this with, so defaulting to fields relating to moment of checking.
        # Source and Tx hash unable to be included due to the aforementioned.             
        else:
            tx_hash = None
            block = chain.eth.blockNumber
            source = None 
            timestamp = pd.Timestamp(datetime.fromtimestamp(chain.eth.getBlock(block).timestamp))
            prev_value = last_tout[1]
            curr_value = tout
        
        if proc:
            # Get input index
            if len(df) == 0:
                idx = 0
            else:
                idx = 1
            # Populate df for tout
            df.at[idx, 'PREV_VALUE'] = prev_value
            df.at[idx, 'CURR_VALUE'] = curr_value
            df.at[idx, 'PARAMETER'] = 'PSM.tout'
            df.at[idx, 'TIMESTAMP'] = timestamp
            df.at[idx, 'TX_HASH'] = tx_hash
            df.at[idx, 'BLOCK'] = block
            df.at[idx, 'SOURCE'] = source
            df.at[idx, 'ILK'] = contract[2]
        
        results.append(df)

    return pd.concat([results[0], results[1], results[2]], axis=0)    


def new_gsm_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3) -> pd.DataFrame:
    """
    Function to fetch new GSM parameters:
        - pause
    """
    # Fetch last updated value
    last_val = engine.cursor().execute("""select max(block), to_value from maker.public.parameters where parameter = 'GSM.pause' group by to_value""").fetchone()
    result = int(chain.eth.getStorageAt(account=Web3.toChecksumAddress('0xbe286431454714f511008713973d3b053a2d38f3'), position=4).hex(), 16)
    
    # If parameter not existent in dataset, create first entry
    if not last_val:
        block = chain.eth.get_transaction('0x60215e5c38f8d02a64b4c029619d9efe88671dd7468ad84997f0812fe0ae6cc6')['blockNumber']
        timestamp = pd.Timestamp(engine.cursor().execute(f"select TIMESTAMP from EDW_SHARE.RAW.BLOCKS where BLOCK = {block}").fetchone()[0])
        tx_hash = '0x60215e5c38f8d02a64b4c029619d9efe88671dd7468ad84997f0812fe0ae6cc6'
        source = '0xbe286431454714f511008713973d3b053a2d38f3'
        prev_value = None
    # Do nothing if last updated value is equal to current. No update needed.
    elif last_val[1] == result:
        return None
    # If value updated, create entry with current block, timestamp and source. 
    # Note: this should be replaced with fields on the parameter-changing tx. 
    # Nothing in database yet to test this with, so defaulting to fields relating to moment of checking.
    # Source and Tx hash unable to be included due to the aforementioned. 
    # Return None if last updated value is equal to current. No update needed.
    else:
        block = chain.eth.blockNumber 
        timestamp = pd.Timestamp(datetime.fromtimestamp(chain.eth.getBlock(block).timestamp))
        source = None 
        tx_hash = None
        prev_value = last_val[1]
    
    # Construct dataframe
    df = pd.DataFrame()
    df.at[0, 'BLOCK'] = block
    df['TIMESTAMP'] = timestamp
    df['TX_HASH'] = tx_hash
    df['PREV_VALUE'] = prev_value
    df['CURR_VALUE'] = result
    df['PARAMETER'] = 'GSM.pause'
    df['SOURCE'] = source
        
    return df

def new_pause_params(engine: snowflake.connector.connection.SnowflakeConnection,
                     chain: web3.main.Web3) -> pd.DataFrame:
    """
    Function to fetch new PAUSE parameters:
        - wait
    """
    # Fetch last updated value
    last_val = engine.cursor().execute("""select max(block), to_value from maker.public.parameters where parameter = 'GSM.pause' group by to_value""").fetchone()
    result = int(chain.eth.getStorageAt(account=Web3.toChecksumAddress('0xBB856d1742fD182a90239D7AE85706C2FE4e5922'), position=9).hex(), 16)

    # If parameter not existent in dataset, create first entry
    if not last_val:
        block = chain.eth.get_transaction('0x4b7d97c3ea9c1977db40eb6f74ac9851da1a3fac4fa68226d929adf66ef94643')['blockNumber']
        timestamp = pd.Timestamp(engine.cursor().execute(f"select TIMESTAMP from EDW_SHARE.RAW.BLOCKS where BLOCK = {block}").fetchone()[0])
        tx_hash = '0x4b7d97c3ea9c1977db40eb6f74ac9851da1a3fac4fa68226d929adf66ef94643'
        source = '0xBB856d1742fD182a90239D7AE85706C2FE4e5922'
        prev_value = None
    # Do nothing if last updated value is equal to current. No update needed.
    elif last_val[1] == result:
        return None
    # If value updated, create entry with current block, timestamp and source. 
    # Note: this should be replaced with fields on the parameter-changing tx. 
    # Nothing in database yet to test this with, so defaulting to fields relating to moment of checking.
    # Source and Tx hash unable to be included due to the aforementioned. 
    # Return None if last updated value is equal to current. No update needed.
    else:
        block = chain.eth.blockNumber 
        timestamp = pd.Timestamp(datetime.fromtimestamp(chain.eth.getBlock(block).timestamp))
        tx_hash = None
        source = None 
        prev_value = last_val[1]
    
    # Construct dataframe
    df = pd.DataFrame()
    df.at[0, 'BLOCK'] = block
    df['TIMESTAMP'] = timestamp
    df['TX_HASH'] = tx_hash
    df['PREV_VALUE'] = prev_value
    df['CURR_VALUE'] = result
    df['PARAMETER'] = 'PAUSE.wait'
    df['SOURCE'] = source
        
    return df

def get_new_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3) -> pd.DataFrame:
    """
    Construct dataframe of parameter additions.
    """
    newesm = new_esm_params(engine, chain)
    newpsm = new_psm_params(engine, chain)
    newflop = new_flopper_params(engine, chain)
    newflap = new_flapper_params(engine, chain)
    newgsm = new_gsm_params(engine, chain)
    newpause = new_pause_params(engine, chain)
    
    # Formatting assurance
    concatenated = pd.concat([newesm, newpsm, newflop, newflap, newgsm, newpause], axis=0)
    concatenated.fillna(value=np.nan, inplace=True)
    concatenated.PREV_VALUE = [int(i) if not math.isnan(i) else None for i in concatenated.PREV_VALUE]
    concatenated.CURR_VALUE = [int(i) if not math.isnan(i) else None for i in concatenated.CURR_VALUE]
    concatenated.BLOCK = [int(i) for i in concatenated.BLOCK]
    concatenated.replace({np.nan: None}, inplace=True)
    concatenated = concatenated.reset_index().drop(columns='index')
    
    # Removing all values with same pre & post values.
    for i in range(len(concatenated)):
        try:      
            if int(concatenated.at[i, 'PREV_VALUE']) == int(concatenated.at[i, 'CURR_VALUE']):
                concatenated.drop(i, inplace=True)
        except:
            continue
        
        
    return concatenated[['TX_HASH','SOURCE','PARAMETER','ILK','TIMESTAMP','BLOCK','PREV_VALUE','CURR_VALUE']]


def upload_new_params(engine: snowflake.connector.connection.SnowflakeConnection,
                   chain: web3.main.Web3) -> pd.DataFrame:
    """
    Generate and upload dataframe of newly obtained parameters.
    """
    result = get_new_params(engine, chain)
    pattern = _write_to_stage(sf, list(result.to_numpy()), f"mcd.internal.TEST_DSPOT_PARAMS_STAGE") 
    _write_to_table(sf,f"mcd.internal.TEST_DSPOT_PARAMS_STAGE",f"maker.public.parameters",pattern)

    return