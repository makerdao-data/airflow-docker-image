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
from connectors.chain import chain
from connectors.eth_calls.token import load_token

ABI = """[{"name":"coins","outputs":[{"type":"address","name":"out"}],"inputs":[{"type":"int128","name":"arg0"}],"constant":true,"payable":false,"type":"function"},
          {"name":"underlying_coins","outputs":[{"type":"address","name":"out"}],"inputs":[{"type":"int128","name":"arg0"}],"constant":true,"payable":false,"type":"function"}]"""


def load_coins(address):

    coins = dict(coins=[], underlying=[])
    try:
        curve_contract = chain.eth.contract(address=Web3.toChecksumAddress(address), abi=ABI)
        i = 0
        while True:
            try:
                address = curve_contract.functions.coins(i).call().lower()
                symbol, decimals = load_token(address)
                coins['coins'].append(dict(address=address, symbol=symbol, decimals=decimals))
                try:
                    underlying_address = curve_contract.functions.underlying_coins(i).call().lower()
                    underlying_symbol, underlying_decimals = load_token(underlying_address)
                    coins['underlying'].append(
                        dict(
                            address=underlying_address, symbol=underlying_symbol, decimals=underlying_decimals
                        )
                    )
                except:
                    coins['underlying'].append(dict(address=address, symbol=symbol, decimals=decimals))
                i += 1
            except:
                break

    except Exception as e:
        print(e)

    return coins
