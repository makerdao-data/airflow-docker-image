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
import os
import sys

sys.path.append(os.environ.get('SYS_PATH'))
from dags.connectors.chain import chain


def get_liquidation_penalty(ilk):

    abi = """[{"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"}],"name":"chop","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]"""

    Dog = chain.eth.contract(
        address=Web3.toChecksumAddress('0x135954d155898D42C90D2a57824C690e0c7BEf1B'), abi=abi
    )
    multiplier = Dog.functions.chop(ilk).call()
    penalty = multiplier - 1 * (10 ** 18)
    penalty = penalty / 10 ** 18

    return penalty


def get_tx(tx_hash):

    tx = chain.eth.get_transaction(tx_hash)

    return tx


def get_receipt(tx_hash):

    r = chain.eth.get_transaction_receipt(tx_hash)

    return r


def get_logs(x):

    # f = chain.eth.filter({'fromBlock': 24250466, 'toBlock': 24250566, 'address': Web3.toChecksumAddress('0x121d0953683f74e9a338d40d9b4659c0ebb539a0')})
    l = chain.eth.getLogs(x)

    return l


def get_data(x):

    new_transaction_filter = chain.eth.filter(x)
    d = new_transaction_filter.get_all_entries()

    return d


def get_breadcrumb(trace_address):

    if len(trace_address) > 0:
        x = ''
        for i in trace_address.split(','):

            x += str('%03d' % int(i)) + '_'

        x = x[:-1]
    else:
        x = '000'

    return x


def read_urn(cdp, block=None):

    abi = """[{"constant": true, "inputs": [{"internalType": "uint256","name": "","type": "uint256"}],
            "name": "urns","outputs": [{"internalType": "address","name": "","type": "address"}],
            "payable": false,"stateMutability": "view","type": "function"}]"""

    cdp_manager = chain.eth.contract(address='0x5ef30b9986345249bc32d8928B7ee64DE9435E39', abi=abi)
    try:
        if block:
            urn = cdp_manager.functions.urns(cdp).call(block_identifier=block)
        else:
            urn = cdp_manager.functions.urns(cdp).call()

        if urn == "0x0000000000000000000000000000000000000000":
            urn = None
        else:
            urn = urn.lower()
    except:
        urn = None

    return urn
