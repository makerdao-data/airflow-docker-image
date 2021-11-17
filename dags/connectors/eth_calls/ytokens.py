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
    '[{"constant":true,"inputs":[],"name":"getPricePerFullShare","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],'
    '"payable":false,"stateMutability":"view","type":"function"}]'
)

# global storage of ytoken rates
ytoken_rates = dict()


# get the daily rate of Yearn token
def load_ytoken_rate(gem, timestamp, block):

    if (gem['address'], timestamp.date()) not in ytoken_rates:
        try:
            token_contract = chain.eth.contract(address=Web3.toChecksumAddress(gem['address']), abi=ABI)
            if block:
                rate = Decimal(
                    token_contract.functions.getPricePerFullShare().call(block_identifier=block) / 10 ** 18
                )
            else:
                rate = Decimal(token_contract.functions.getPricePerFullShare().call() / 10 ** 18)

        except Exception as e:
            rate = None
            print(e)

        ytoken_rates[(gem['address'], timestamp.date())] = rate

    return ytoken_rates[(gem['address'], timestamp.date())]
