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

ABI = """[{"constant": true,"inputs": [],"name": "decimals","outputs": [{"internalType": "uint8","name": "","type": "uint8"}],
           "payable": false,"stateMutability": "view","type": "function"},
          {"constant": true,"inputs": [],"name": "symbol","outputs": [{"internalType": "string","name": "","type": "string"}],
           "payable": false,"stateMutability": "view","type": "function"},
          {"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
           "payable":false,"stateMutability":"view","type":"function"}]"""


def load_token(address):

    try:
        token_contract = chain.eth.contract(address=Web3.toChecksumAddress(address), abi=ABI)
        symbol = token_contract.functions.symbol().call().replace("'", "")
        decimals = token_contract.functions.decimals().call()
    except Exception as e:
        print(e)
        symbol = address[:8]
        decimals = 18

    return symbol, decimals


def balance_of(token, address, block=None):

    try:
        token_contract = chain.eth.contract(address=Web3.toChecksumAddress(token), abi=ABI)
        if block:
            balance = token_contract.functions.balanceOf(Web3.toChecksumAddress(address)).call(
                block_identifier=block
            )
        else:
            balance = token_contract.functions.balanceOf(Web3.toChecksumAddress(address)).call()
        decimals = token_contract.functions.decimals().call()
        balance /= 10 ** decimals
    except Exception as e:
        print(e)
        balance = None

    return balance
