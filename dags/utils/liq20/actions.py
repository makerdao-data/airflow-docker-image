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
import json
from airflow.exceptions import AirflowFailException
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, _write_actions_to_table, _write_to_stage, _clear_stage
from dags.utils.mcd_units import wad, ray, rad
from dags.utils.logs_decoder import decode_logs
from dags.connectors.gcp import bq_query


def get_calc(DB, address):

    calc_abi = [
        {
            "inputs": [
                {"internalType": "uint256", "name": "top", "type": "uint256"},
                {"internalType": "uint256", "name": "dur", "type": "uint256"},
            ],
            "name": "price",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }
    ]

    calc_address = sf.execute(
        f"""select calc
                                from {DB}.internal.clipper
                                where lower(clip) = '{address.lower()}'; """
    ).fetchone()[0]

    try:
        calc = chain.eth.contract(address=Web3.toChecksumAddress(calc_address), abi=calc_abi)
    except Exception as e:
        print(e)
        calc = None

    return calc


def get_ilk(DB, address):

    q = f"""select ilk
        from {DB}.internal.clipper
        where lower(clip) = '{address.lower()}'; """

    ilk = sf.execute(q).fetchone()

    if ilk:
        i = ilk[0]
    else:
        i = None

    return i


def get_clipper_actions(load_id, start_block, end_block, start_time, end_time, DB, STAGING):

    clippers = sf.execute(f"""select clip from {DB}.internal.clipper; """).fetchall()
    clippers_list = ','.join(["'%s'" % clipper[0].lower() for clipper in clippers])

    q = f"""SELECT *
            FROM `bigquery-public-data.crypto_ethereum.logs`
            WHERE block_number >= {start_block} AND block_number <= {end_block}
                AND date(block_timestamp) >= '{start_time[:10]}' AND date(block_timestamp) <= '{end_time[:10]}'
                AND address in ({clippers_list})
                AND topics[offset(0)] in ('0x7c5bfdc0a5e8192f6cd4972f382cec69116862fb62e6abff8003874c58e064b8',
                                            '0x05e309fd6ce72f2ab888a20056bb4210df08daed86f21f95053deb19964d86b1',
                                            '0x275de7ecdd375b5e8049319f8b350686131c219dd4dc450a08e9cf83b03c865f')
            ORDER BY block_number, transaction_index, log_index;"""

    logs = decode_logs(bq_query(q))

    if start_block:
        d = sf.execute(
            f"""select *
                from {DB}.staging.clip
                where block >= {start_block}
                    and function in ('kick', 'take', 'redo')
                order by block, tx_index"""
        ).fetchall()
    else:
        d = sf.execute(
            f"""select *
                from {DB}.staging.clip
                where function in ('kick', 'take', 'redo')
                order by block, tx_index"""
        ).fetchall()

    # CREATE A DICT CONTAINING INITIAL PRICES FOR ALL AUCTIONS
    all_init_prices = sf.execute(
        f"""select ilk, auction_id, timestamp, init_price
            from {DB}.internal.action
            where type = 'kick' and status = 1; """
    ).fetchall()

    init_prices_dict = dict()
    for i in all_init_prices:
        init_prices_dict[i[0]] = {i[1]: [i[2], int(i[3] * (10 ** 27))]}

    actions = []
    for i in d:

        args = json.loads(i[11])
        outputs = json.loads(i[12])
        tx_hash = i[4]
        block = i[1]
        tx_index = i[5]

        if i[10] in ['kick', 'take', 'redo']:

            data = None  # take
            max_collateral_amt = None  # take
            max_price = None  # take
            available_collateral = None  # take / redo / kick
            sold_collateral = None  # take
            recovered_debt = None  # take
            collateral_price = None  # take
            closing_take = 0  # take
            init_price = None  # kick / redo
            keeper = None  # kick / redo
            incentives = None  # kick / redo
            debt = None  # take / redo / kick
            urn = None
            ilk = get_ilk(DB, i[9])

            osm_price, mkt_price = sf.execute(
                f"""select osm_price, mkt_price
                    from mcd.internal.prices
                    where block = {i[1]}
                    and token = '{ilk.split('-')[0]}'; """
            ).fetchone()

            try:

                if i[10] == 'kick':

                    # TODO: add checks between logs and data that are already in sf
                    log = None
                    if i[14] == 1:
                        for item in logs:
                            if (
                                item['usr'].lower() == args[2]['value'].lower()
                                and int(item['id']) == int(outputs[0]['value'])
                                and item['tx_hash'].lower() == tx_hash.lower()
                                and int(item['block']) == int(block)
                                and item['event'] == 'kick'
                            ):
                                log = item

                    if log:
                        auction_id = log['id']
                        available_collateral = wad(log['lot'])  # / 10**18
                        debt = rad(log['tab'])  # / 10**45
                        init_price = ray(log['top'])  # / 10**27
                        keeper = log['kpr']
                        incentives = rad(log['coin'])  # / 10**45
                        urn = log['usr']
                        collateral_price = init_price

                        # ADD INITIAL PRICE OF NEW AUCTION TO A DICT
                        init_prices_dict.setdefault(ilk, {auction_id: []})
                        init_prices_dict[ilk][auction_id] = [i[2], log['top']]

                    else:
                        auction_id = None
                        available_collateral = wad(args[1]["value"])
                        debt = rad(args[0]["value"])
                        urn = args[2]["value"]
                        keeper = args[3]["value"]

                elif i[10] == 'take':

                    auction_id = args[0]['value']
                    max_collateral_amt = wad(args[1]['value'])  # / 10**18
                    max_price = ray(args[2]['value'])  # / 10**27
                    data = args[4]['value']

                    log = None
                    if i[14] == 1:
                        for item in logs:
                            if (
                                int(item['id']) == int(args[0]['value'])
                                and item['tx_hash'].lower() == tx_hash.lower()
                                and int(item['block']) == int(block)
                                and item['event'] == 'take'
                            ):
                                log = item

                    if log:
                        available_collateral = wad(log['lot'])  # / 10**18
                        recovered_debt = rad(log['owe'])  # / 10**45
                        debt = rad(log['tab'])  # / 10**45
                        collateral_price = ray(log['price'])  # / 10**27
                        # check if collateral price != 0
                        # TODO: add warning that smth is wrong
                        if collateral_price and collateral_price != 0:
                            sold_collateral = (rad(log['owe'])) / (ray(log['price']))
                        else:
                            sold_collateral = 0
                        if log['tab'] == 0:
                            closing_take = 1
                        urn = log['usr']

                    else:

                        try:
                            initial_auction_data = init_prices_dict[ilk][auction_id]
                            initial_auction_price = initial_auction_data[1]
                            seconds_passed = i[2] - initial_auction_data[0]
                            c = get_calc(DB, i[9])
                            collateral_price = c.functions.price(
                                initial_auction_price, seconds_passed.seconds
                            ).call()
                            collateral_price = ray(collateral_price)  # / 10**27
                        except Exception as e:
                            print(e)
                            print('#ERRR: AUCTION DOES NOT EXIST')

                elif i[10] == 'redo':

                    auction_id = args[0]['value']
                    keeper = args[1]['value']

                    log = None
                    if i[14] == 1:
                        for item in logs:
                            if (
                                int(item['id']) == int(args[0]['value'])
                                and item['tx_hash'].lower() == tx_hash.lower()
                                and int(item['block']) == int(block)
                                and item['event'] == 'redo'
                            ):
                                log = item

                    if log:
                        available_collateral = wad(log['lot'])  # / 10**18
                        debt = rad(log['tab'])  # / 10**45
                        init_price = ray(log['top'])  # / 10**27
                        keeper = log['kpr']
                        incentives = rad(log['coin'])  # / 10**45
                        urn = log['usr']

                    # get current collateral price
                    try:
                        initial_auction_data = init_prices_dict[ilk][auction_id]
                        initial_auction_price = initial_auction_data[1]
                        seconds_passed = i[2] - initial_auction_data[0]
                        c = get_calc(DB, i[9])
                        collateral_price = c.functions.price(
                            initial_auction_price, seconds_passed.seconds
                        ).call()
                        collateral_price = ray(collateral_price)  # / 10**27
                    except Exception as e:
                        print(e)
                        print('#ERRR: AUCTION DOES NOT EXIST')

                else:

                    pass

            except Exception as e:
                print(e)
                print(i)
                raise AirflowFailException("#ERROR ON GATHERING DATA")

            try:
                actions.append(
                    [
                        load_id,
                        auction_id,
                        i[2],
                        i[1],
                        i[4],
                        i[10],
                        i[8],
                        data,
                        debt,
                        init_price,
                        max_collateral_amt,
                        available_collateral,
                        sold_collateral,
                        max_price,
                        collateral_price,
                        osm_price,
                        recovered_debt,
                        closing_take,
                        keeper,
                        incentives,
                        urn,
                        i[15],
                        i[14],
                        i[13],
                        None,
                        i[3],
                        ilk,
                        mkt_price
                    ]
                )
            except Exception as e:
                print(e)
                print(i)
                raise AirflowFailException("#ERROR ON PREPARING DATA TO LOAD")

    if len(actions) > 0:
        # if custom_write_to_stage(actions, f"{DB}.staging.liquidations_extracts"):
        #     write_actions_to_table(f"{DB}.staging.liquidations_extracts", f"{DB}.internal.action")

        pattern = _write_to_stage(sf, actions, f"{DB}.staging.liquidations_extracts")
        if pattern:
            _write_actions_to_table(
                sf,
                f"{DB}.staging.liquidations_extracts",
                f"{DB}.internal.action",
                pattern,
            )
            _clear_stage(sf, f"{DB}.staging.liquidations_extracts", pattern)

    print('{} rows loaded.'.format(len(actions)))

    return True
