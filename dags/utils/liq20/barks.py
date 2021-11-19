#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from web3 import Web3
import json
from dags.connectors.sf import sf, sf_dict, _write_to_stage, _write_barks_to_table, _clear_stage
from dags.connectors.chain import chain
from dags.utils.bc_adapters import get_liquidation_penalty
from dags.utils.mcd_units import wad, rad, ray


# add ILK to params
def get_vault_id(urn, ilk):

    query = f"""select vault
            from mcd.internal.vault_operations
            where urn = '{urn}' and ilk = '{ilk}'; """

    vault = sf.execute(query).fetchone()
    if vault:
        v = vault[0]
    else:
        v = None

    return v


def get_vault_info(urn, ilk):

    vault_info = dict(RATIO=None, OWNER=None, OSM_PRICE=None, MKT_PRICE=None)

    if ilk:
        query = f"""select ratio, owner, osm_price, mkt_price
                    from mcd.public.current_vaults
                    where lower(urn) = '{urn.lower()}'
                        and ilk = '{ilk}'; """

        result = sf_dict.execute(query).fetchone()
        if result:
            vault_info = result

    return vault_info['RATIO'], vault_info['OWNER'], vault_info['OSM_PRICE'], vault_info['MKT_PRICE']


def get_Bark(tx_hash, urn, ilk, auction_id):

    dog_abi = json.loads(
        """[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"ilk","type":"bytes32"},{"indexed":true,"internalType":"address","name":"urn","type":"address"},{"indexed":false,"internalType":"uint256","name":"ink","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"art","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"due","type":"uint256"},{"indexed":false,"internalType":"address","name":"clip","type":"address"},{"indexed":true,"internalType":"uint256","name":"id","type":"uint256"}],"name":"Bark","type":"event"}]"""
    )
    Dog = chain.eth.contract(
        address=Web3.toChecksumAddress('0x135954d155898D42C90D2a57824C690e0c7BEf1B'), abi=dog_abi
    )
    receipt = chain.eth.getTransactionReceipt(tx_hash)
    logs = Dog.events.Bark().processReceipt(receipt)

    log = None
    for i in logs:

        log_ilk = Web3.toText(i['args']['ilk']).rstrip('\x00')
        log_urn = i['args']['urn'].lower()
        log_id = i['args']['id']

        if log_ilk == ilk and log_urn == urn.lower() and log_id == auction_id:

            log = i

    Bark_ink = wad(log['args']['ink'])
    Bark_due = rad(log['args']['due'])

    return Bark_ink, Bark_due


def get_barks(load_id, start_block, end_block, start_time, end_time, DB, STAGING):

    if start_block:
        barks = sf.execute(
            f"""select block, timestamp, tx_hash, from_address, arguments, outputs, error, status, gas_used
                            from {DB}.staging.dog
                            where function = 'bark'
                            and block >= {start_block}
                            order by block, breadcrumb; """
        ).fetchall()
    else:
        barks = sf.execute(
            f"""select block, timestamp, tx_hash, from_address, arguments, outputs, error, status, gas_used
                            from {DB}.staging.dog
                            where function = 'bark'
                            order by block, breadcrumb; """
        ).fetchall()

    brk = []
    for i in barks:

        args = json.loads(i[4])
        out = json.loads(i[5])

        auction_id = None
        if out:
            auction_id = out[0]['value']
        try:
            ilk = Web3.toText(args[0]['value']).rstrip('\x00')
        except Exception as e:
            print(e)
            ilk = None
        urn = args[1]['value']
        tx_hash = i[2]

        vault_info = get_vault_info(urn, ilk)
        Bark = [None, None]
        if i[7] == 1:
            Bark = get_Bark(tx_hash, urn, ilk, auction_id)

        block = i[0]
        timestamp = i[1]
        vault_id = get_vault_id(urn, ilk)
        ilk = ilk
        owner = vault_info[1]
        collateral = Bark[0]
        debt = Bark[1]
        liq_penalty = get_liquidation_penalty('0x' + str(args[0]['value']))
        liq_ratio = vault_info[0]
        osm_price = vault_info[2]
        mkt_price = vault_info[3]
        auction_id = auction_id
        caller = i[3]
        keeper = args[2]['value']
        gas_used = i[8]
        status = i[7]
        revert_reason = i[6]

        brk.append(
            [
                load_id,
                block,
                timestamp,
                tx_hash,
                urn,
                vault_id,
                ilk,
                owner,
                collateral,
                debt,
                liq_penalty,
                liq_ratio,
                osm_price,
                mkt_price,
                auction_id,
                caller,
                keeper,
                gas_used,
                status,
                revert_reason,
            ]
        )

    if len(brk) > 0:
        pattern = _write_to_stage(sf, brk, f"{DB}.staging.liquidations_extracts")
        if pattern:
            _write_barks_to_table(
                sf,
                f"{DB}.staging.liquidations_extracts",
                f"{DB}.internal.bark",
                pattern,
            )
            _clear_stage(sf, f"{DB}.staging.liquidations_extracts", pattern)

    print('{} rows loaded'.format(len(brk)))

    return True
