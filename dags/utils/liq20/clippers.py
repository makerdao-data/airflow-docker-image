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
from airflow.exceptions import AirflowFailException
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.chain import chain
from dags.connectors.sf import sf, _write_to_stage, _write_to_table, _clear_stage
from dags.utils.general import breadcrumb


def update_clippers(**setup):

    created_clippers = sf.execute(f"""
        select block, timestamp, tx_hash, call_id, call_data::varchar as call_data, concat('0x', lpad(ltrim(lower(return_value), '0x'), 40, '0')) as return_value, order_index
        from edw_share.raw.calls
        where to_address = lower('0x0716F25fBaAae9b63803917b6125c10c313dF663')
        and left(call_data, 10) = '0xbf5f804e'
        and block >= {setup['start_block']}
        and block <= {setup['end_block']}
        and status;
    """).fetchall()

    c = list()

    clip_abi = [
        {
            "inputs": [],
            "name": "ilk",
            "outputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "calc",
            "outputs": [{"internalType": "contract AbacusLike", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    for block, timestamp, tx_hash, call_id, call_data, return_value, order_index in created_clippers:

        Clip = chain.eth.contract(address=Web3.toChecksumAddress(return_value), abi=clip_abi)
        calc = Clip.functions.calc().call()

        c.append([
            block,
            timestamp,
            tx_hash,
            breadcrumb(call_id),
            return_value.lower(), # clipper
            Web3.toText(call_data[266:]).rstrip('\x00'), # ilk
            calc.lower(),
            order_index
        ])

    pattern = None
    if c:
        pattern = _write_to_stage(sf, c, f"{setup['STAGING']}")

        _write_to_table(
            sf,
            f"{setup['STAGING']}",
            f"{setup['DB']}.INTERNAL.CLIPPER",
            pattern,
        )
        _clear_stage(sf, f"{setup['STAGING']}", pattern)
