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

from decimal import Decimal
from datetime import datetime, timezone
from web3 import Web3
from connectors.chain import chain


# read list of offers from _dex_ orderbook for pair (_pay_token_ - _buy_token_) at _block_
def read_offers(support_contract, dex, pay_token, buy_token, block):

    # normalize addresses
    dex = Web3.toChecksumAddress(dex)
    pay_token = Web3.toChecksumAddress(pay_token)
    buy_token = Web3.toChecksumAddress(buy_token)

    offers = []
    ids = pay_amts = buy_amts = owners = timestamps = None
    offer_id = 0

    # read first batch of offers
    if block:
        try:
            ids, pay_amts, buy_amts, owners, timestamps = support_contract.functions.getOffers(
                dex, pay_token, buy_token
            ).call(block_identifier=block)
        except Exception as e:
            ids = None

    # parse the batch of offers
    while ids:

        for offer_id, pay_amt, buy_amt, owner, timestamp in zip(ids, pay_amts, buy_amts, owners, timestamps):
            if offer_id:
                offers.append(
                    dict(
                        offer_id=offer_id,
                        tick=datetime.utcfromtimestamp(timestamp).replace(tzinfo=timezone.utc),
                        maker=owner.lower(),
                        pay_gem=pay_token.lower(),
                        buy_gem=buy_token.lower(),
                        pay_amt=pay_amt,
                        buy_amt=buy_amt,
                    )
                )

        # if last offer_id == 0 then it was the last batch so break the loop
        if not offer_id:
            break

        # get the next batch of offers
        try:
            ids, pay_amts, buy_amts, owners, timestamps = support_contract.functions.getOffers(
                dex, offer_id
            ).call(block_identifier=block)
            # skip the first offer (it was already processed with a previous batch)
            ids.pop(0)
            pay_amts.pop(0)
            buy_amts.pop(0)
            owners.pop(0)
            timestamps.pop(0)
        except Exception as e:
            ids = None
            offers = []

    return offers


# read orderbook for _dex_ and pair (_pay_token_ - _buy_token_) at _block_ or now
def read_orderbook(dex, base_token, quote_token, block=None):

    support_contract_address = '0x9b3F075b12513afe56Ca2ED838613B7395f57839'
    abi = (
        '[{"constant":true,"inputs":[{"name":"otc","type":"address"},{"name":"payToken","type":"address"},{"name":"buyToken","type":"address"}],'
        '"name":"getOffers","outputs":[{"name":"ids","type":"uint256[100]"},{"name":"payAmts","type":"uint256[100]"},'
        '{"name":"buyAmts","type":"uint256[100]"},{"name":"owners","type":"address[100]"},{"name":"timestamps","type":"uint256[100]"}],'
        '"payable":false,"stateMutability":"view","type":"function"},'
        '{"constant":true,"inputs":[{"name":"otc","type":"address"},{"name":"offerId","type":"uint256"}],"name":"getOffers",'
        '"outputs":[{"name":"ids","type":"uint256[100]"},{"name":"payAmts","type":"uint256[100]"},{"name":"buyAmts","type":"uint256[100]"},'
        '{"name":"owners","type":"address[100]"},{"name":"timestamps","type":"uint256[100]"}],'
        '"payable":false,"stateMutability":"view","type":"function"}]'
    )

    support_contract = chain.eth.contract(address=Web3.toChecksumAddress(support_contract_address), abi=abi)
    try:
        if not block:
            block = chain.eth.blockNumber
    except:
        block = None

    offers = dict()

    if block:

        # read and process buy side of the orderbook
        sell_offers = read_offers(support_contract, dex[0], quote_token[0], base_token[0], block)
        for offer in sell_offers:
            offer['type'] = 'sell'
            offer["base_amt"] = offer["buy_amt"] / Decimal(10 ** base_token[1])
            offer["quote_amt"] = offer["pay_amt"] / Decimal(10 ** quote_token[1])
            offer['price'] = offer["quote_amt"] / offer["base_amt"] if offer["base_amt"] else None

            offers[offer['offer_id']] = dict(
                offer_id=offer['offer_id'],
                tick=offer['tick'],
                maker=offer['maker'],
                base_gem=base_token[0],
                quote_gem=quote_token[0],
                base_amt=offer['base_amt'],
                quote_amt=offer['quote_amt'],
                type=offer['type'],
                price=offer['price'],
            )

        # read and process sell side of the orderbook
        buy_offers = read_offers(support_contract, dex[0], base_token[0], quote_token[0], block)
        for offer in buy_offers:
            offer['type'] = 'buy'
            offer["base_amt"] = offer["pay_amt"] / Decimal(10 ** base_token[1])
            offer["quote_amt"] = offer["buy_amt"] / Decimal(10 ** quote_token[1])
            offer['price'] = offer["quote_amt"] / offer["base_amt"] if offer["base_amt"] else None

            offers[offer['offer_id']] = dict(
                offer_id=offer['offer_id'],
                tick=offer['tick'],
                maker=offer['maker'],
                base_gem=base_token[0],
                quote_gem=quote_token[0],
                base_amt=offer['base_amt'],
                quote_amt=offer['quote_amt'],
                type=offer['type'],
                price=offer['price'],
            )

    return offers
