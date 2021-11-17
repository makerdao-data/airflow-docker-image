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

ABI = """[{"constant":true,"inputs":[],"name":"getFinalTokens","outputs":[{"internalType":"address[]","name":"tokens","type":"address[]"}],
          "payable":false,"stateMutability":"view","type":"function"},
          {"constant":true,"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"getBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
          "payable":false,"stateMutability":"view","type":"function"}]"""


def load_coins(address):

    coins = []
    try:
        balancer_contract = chain.eth.contract(address=Web3.toChecksumAddress(address), abi=ABI)
        tokens = balancer_contract.functions.getFinalTokens().call()
        for token in tokens:
            coins.append(dict(address=token.lower()))
    except Exception as e:
        print(e)

    return coins
