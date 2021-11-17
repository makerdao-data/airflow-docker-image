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
from decimal import Decimal
from connectors.chain import chain

ABI = (
    '[{"constant":true,"inputs":[],"name":"exchangeRateStored","outputs":[{"internalType":"uint256","name":"","type":"uint256"}], '
    '"payable":false,"stateMutability":"view","type":"function"}]'
)

# global storage of ctoken rates
ctoken_rates = dict()


# get the daily rate of Compound token
def load_ctoken_rate(gem, timestamp, block):

    if (gem['address'], timestamp.date()) not in ctoken_rates:
        try:
            token_contract = chain.eth.contract(address=Web3.toChecksumAddress(gem['address']), abi=ABI)
            if block:
                rate = Decimal(
                    token_contract.functions.exchangeRateStored().call(block_identifier=block) / 10 ** 28
                )
            else:
                rate = Decimal(token_contract.functions.exchangeRateStored().call() / 10 ** 28)
        except Exception as e:
            rate = None
            print(e)

        ctoken_rates[(gem['address'], timestamp.date())] = rate

    return ctoken_rates[(gem['address'], timestamp.date())]
