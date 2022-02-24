from web3 import Web3
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, _write_to_stage, _write_to_table, _clear_stage
from dags.connectors.chain import chain
from datetime import datetime


def _get_admins():
        
    start_block = sf.execute("""
        select max(end_block)
        from mcd.internal.admin_scheduler;
    """).fetchone()[0]

    if not start_block:
        start_block = 0
        
    end_block = sf.execute("""
        select max(block)
        from edw_share.raw.storage_diffs;
    """).fetchone()[0]

    if end_block > start_block:

        all_managers_dict = dict()

        # CDP Manager
        # 2 urn / 4 ds proxy -- exclude 0xddb108893104de4e1c6d0e47c42237db4e617acc (Maker Deployer contract)  / 5 ilk
        cdpmanager = sf.execute(f"""
            select distinct substr(location, 0, 1) loc, substr(location, 3, length(location) - 5) as id, curr_value as vault_param
            from edw_share.raw.storage_diffs
            where block > {start_block} and block <= {end_block} and
                contract = '0x5ef30b9986345249bc32d8928b7ee64de9435e39' and 
                (location like '2[%].0' or location like '4[%].0' or location like '5[%].0') and
                status
            order by id, loc;
        """).fetchall()



        for loc, id, vault_param in cdpmanager:

            if vault_param == '0xddb108893104de4e1c6d0e47c42237db4e617acc':
                pass
            else:

                all_managers_dict.setdefault(id, {})
                if str(loc) == '2':

                    
                    all_managers_dict[id].setdefault('urn', vault_param)
                    all_managers_dict[id].setdefault('id', id)
                
                elif str(loc) == '4':
                    
                    all_managers_dict[id].setdefault('ds_proxy', vault_param)

                elif str(loc) == '5':
                    
                    all_managers_dict[id].setdefault('ilk', bytes.fromhex(vault_param[2:]).decode('utf-8').rstrip('\x00'))
                
                else:

                    pass
            

        # BCDP Manager
        # 2 urn / 4 ds proxy -- exclude 0x4bcad4920be1ca53f27656db49d31b23f9725ab0 (Maker Deployer contract)  / 5 ilk

        bcdpmanager = sf.execute(f"""
            select distinct substr(location, 0, 1) loc, concat('B-', substr(location, 3, length(location) - 5)) as id, curr_value as vault_param
            from edw_share.raw.storage_diffs
            where block > {start_block} and block <= {end_block} and
                contract = '0x3f30c2381cd8b917dd96eb2f1a4f96d91324bbed' and 
                (location like '2[%].0' or location like '4[%].0' or location like '5[%].0') and
                status
            order by id, loc;
        """).fetchall()


        for loc, id, vault_param in bcdpmanager:

            if vault_param == '0x4bcad4920be1ca53f27656db49d31b23f9725ab0':
                pass
            else:
                
                all_managers_dict.setdefault(id, {})
                if str(loc) == '2':

                    all_managers_dict[id].setdefault('urn', vault_param)
                    all_managers_dict[id].setdefault('id', id)

                
                elif str(loc) == '4':
                    
                    all_managers_dict[id].setdefault('ds_proxy', vault_param)

                elif str(loc) == '5':
                    
                    all_managers_dict[id].setdefault('ilk', bytes.fromhex(vault_param[2:]).decode('utf-8').rstrip('\x00'))
                
                else:

                    pass
        
            
        # OasisCDP Manager

        oasiscdpmanager = sf.execute(f"""
            select distinct concat('O-', substr(curr_value, 3, 8)) as id, curr_value as urn, substr(location, 51, 64) as ilk, substr(location,3, 42) as ds_proxy
            from edw_share.raw.storage_diffs
            where block > {start_block} and block <= {end_block} and
                contract = '0x60762005be465901ca18ba34416b35143de72c0c' and 
                location like '1[%' and 
                status;
        """).fetchall()


        for id, urn, ilk, ds_proxy in oasiscdpmanager:

            all_managers_dict.setdefault(id, {})
            all_managers_dict[id].setdefault('urn', urn)
            all_managers_dict[id].setdefault('id', id)
            all_managers_dict[id].setdefault('ilk', bytes.fromhex(ilk).decode('utf-8').rstrip('\x00'))
            all_managers_dict[id].setdefault('ds_proxy', ds_proxy)


        vaults_admin = list()
        # list pattern: vault (id), vault owner (from chain), ds proxy, urn, ilk

        for id in all_managers_dict:

            abi = """[{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]"""
            owner = None
            try:
                ds_proxy_contract = chain.eth.contract(address=Web3.toChecksumAddress(all_managers_dict[id]['ds_proxy']), abi=abi)
                owner = ds_proxy_contract.functions.owner().call()
                owner = owner.lower()
            except:
                pass

            vaults_admin.append([
                all_managers_dict[id]['id'],
                owner,
                all_managers_dict[id]['ds_proxy'] if 'ds_proxy' in all_managers_dict[id] else None,
                all_managers_dict[id]['urn'],
                all_managers_dict[id]['ilk'],
            ])


        pattern = _write_to_stage(sf, vaults_admin, f"mcd.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf,
                f"mcd.staging.vaults_extracts",
                f"mcd.public.admin",
                pattern,
            )
            _clear_stage(sf, f"mcd.staging.vaults_extracts", pattern)

        sf.execute(f"""
            insert into mcd.internal.admin_scheduler(start_block, end_block, load_id)
            select {start_block}, {end_block}, '{datetime.utcnow().__str__()[:19]}';
        """)
    
    return