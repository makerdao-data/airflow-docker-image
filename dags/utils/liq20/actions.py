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
from datetime import datetime, timedelta
from connectors.chain import chain
import json
from airflow.exceptions import AirflowFailException
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, _write_actions_to_table, _write_to_stage, _clear_stage
from dags.utils.mcd_units import wad, ray, rad
from dags.utils.logs_decoder import decode_logs
from dags.connectors.gcp import bq_query


def get_calc(address):

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

    try:
        calc = chain.eth.contract(address=Web3.toChecksumAddress(address), abi=calc_abi)
    except Exception as e:
        print(e)
        calc = None

    return calc


def get_clipper_actions(**setup):

    clippers = sf.execute(f"""select clip from {setup['DB']}.internal.clipper; """).fetchall()
    clippers_list = ','.join(["'%s'" % clipper[0].lower() for clipper in clippers])

    q = f"""
        SELECT block, tx_hash, concat('0x', lpad(ltrim(lower(topic0), '0x'), 64, '0')) as function,
            topic1 as auction_id,
            concat('0x', lpad(ltrim(lower(topic2), '0x'), 40, '0')) as urn,
            concat('0x', lpad(ltrim(lower(topic3), '0x'), 40, '0')) as keeper,
            log_data
        FROM edw_share.raw.events
        WHERE block >= {setup['start_block']}
            AND block <= {setup['end_block']}
            AND contract in ({clippers_list})
            AND concat('0x', lpad(ltrim(lower(topic0), '0x'), 64, '0')) in (
                '0x7c5bfdc0a5e8192f6cd4972f382cec69116862fb62e6abff8003874c58e064b8',
                '0x05e309fd6ce72f2ab888a20056bb4210df08daed86f21f95053deb19964d86b1',
                '0x275de7ecdd375b5e8049319f8b350686131c219dd4dc450a08e9cf83b03c865f'
            ) AND status
        ORDER BY block, order_index, log_index;
    """

    logs = sf.execute(q).fetchall()

    all_p = sf.execute(f"""
        select block, token, osm_price
        from mcd.internal.prices
        where block >= {setup['start_block']}
        and block <= {setup['end_block']};
    """).fetchall()

    ppp = dict()
    for block, ilk, price in all_p:

        ppp.setdefault(ilk, {})
        ppp[ilk][block] = dict(
            price=price
        )
    
    all_c = sf.execute(f"""
        select clip, ilk, calc
        from {setup['DB']}.internal.clipper;
    """).fetchall()

    ccc = dict()
    for clip, ilk, calc in all_c:
        
        ccc.setdefault(clip, {})
        ccc[clip]['ilk'] = ilk
        ccc[clip]['calc'] = calc

    if {setup['start_block']}:
        d = sf.execute(
            f"""select load_id, block, timestamp, breadcrumb, tx_hash,
                    tx_index, type, value, from_address, to_address,
                    function, arguments, outputs, error, status, gas_used
                from {setup['DB']}.staging.clip
                where block >= {setup['start_block']}
                    and block <= {setup['end_block']}
                    and function in ('kick', 'take', 'redo')
                order by block, tx_index;"""
        ).fetchall()
    else:
        d = sf.execute(
            f"""select load_id, block, timestamp, breadcrumb, tx_hash,
                    tx_index, type, value, from_address, to_address,
                    function, arguments, outputs, error, status, gas_used
                from {setup['DB']}.staging.clip
                where function in ('kick', 'take', 'redo')
                    and block >= {setup['start_block']}
                    and block <= {setup['end_block']}
                order by block, tx_index;"""
        ).fetchall()

    # CREATE A DICT CONTAINING INITIAL PRICES FOR ALL AUCTIONS
    all_init_prices = sf.execute(
        f"""select ilk, auction_id, timestamp, init_price
            from {setup['DB']}.internal.action
            where type = 'kick'
                and status = 1
                and init_price is not null; """
    ).fetchall()

    init_prices_dict = dict()
    for ilk, auction_id, timestamp, init_price in all_init_prices:
        init_prices_dict[ilk] = {auction_id: [timestamp, int(init_price * (10 ** 27))]}

    actions = []
    for load_id, block, timestamp, breadcrumb, tx_hash, tx_index, type, value, from_address, to_address, function, call_arguments, call_outputs, error, status, gas_used in d:

        args = json.loads(call_arguments)
        outputs = json.loads(call_outputs)
        tx_hash = tx_hash
        block = block
        tx_index = tx_index

        if function in ['kick', 'take', 'redo']:

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
            ilk = ccc[to_address]['ilk']

            osm_price = ppp[ilk.split('-')[0]][block]['price']
            mkt_price = ppp[ilk.split('-')[0]][block]['price']

            try:
                
                if function == 'kick':

                    # TODO: add checks between logs and data that are already in sf
                    log = None
                    if status == 1:


                        for log_block, log_tx_hash, log_function, log_auction_id, log_usr, log_keeper, log_data in logs:

                            if (
                                log_usr.lower() == args[2]['value'].lower()
                                and int(log_auction_id, 16) == int(outputs[0]['value'])
                                and log_tx_hash.lower() == tx_hash.lower()
                                and log_block == int(block)
                                and log_function == '0x7c5bfdc0a5e8192f6cd4972f382cec69116862fb62e6abff8003874c58e064b8'
                            ):
                                log = dict(
                                    id=int(log_auction_id, 16),
                                    lot=int(log_data[130:194], 16),
                                    tab=int(log_data[66:130], 16),
                                    top=int(log_data[2:66], 16),
                                    kpr=log_keeper,
                                    coin=int(log_data[194:258], 16),
                                    usr=log_usr
                                )

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
                        init_prices_dict[ilk][auction_id] = [timestamp, log['top']]

                    else:

                        auction_id = None
                        available_collateral = wad(args[1]["value"])
                        debt = rad(args[0]["value"])
                        urn = args[2]["value"]
                        keeper = args[3]["value"]

                elif function == 'take':

                    auction_id = args[0]['value']
                    max_collateral_amt = wad(args[1]['value'])  # / 10**18
                    max_price = ray(args[2]['value'])  # / 10**27
                    data = args[4]['value']

                    log = None

                    if status == 1:

                        for log_block, log_tx_hash, log_function, log_auction_id, log_usr, log_keeper, log_data in logs:

                            if (
                                int(log_auction_id, 16) == int(args[0]['value'])
                                and log_tx_hash.lower() == tx_hash.lower()
                                and log_block == int(block)
                                and log_function == '0x05e309fd6ce72f2ab888a20056bb4210df08daed86f21f95053deb19964d86b1'
                            ):
                                log = dict(
                                    id=int(log_auction_id, 16),
                                    owe=int(log_data[130:194], 16),
                                    price=int(log_data[66:130], 16),
                                    top=int(log_data[2:66], 16),
                                    kpr=log_keeper,
                                    tab=int(log_data[194:258], 16),
                                    lot=int(log_data[258:322], 16),
                                    usr=log_usr
                                )

                    if log:

                        available_collateral = wad(log['lot'])  # / 10**18
                        recovered_debt = rad(log['owe'])  # / 10**45
                        debt = rad(log['tab'])  # / 10**45
                        collateral_price = ray(log['price'])  # / 10**27
                        if collateral_price and collateral_price != 0:
                            sold_collateral = (rad(log['owe'])) / (ray(log['price']))
                        else:
                            sold_collateral = 0
                        if log['tab'] == 0:
                            closing_take = 1
                        urn = log['usr']


                        initial_auction_data = init_prices_dict[ilk][auction_id]
                        initial_auction_price = initial_auction_data[1]
                        seconds_passed = timestamp - initial_auction_data[0]
                        c = get_calc(ccc[to_address]['calc'])
                        collateral_price = c.functions.price(
                            initial_auction_price, seconds_passed.seconds
                        ).call()
                        collateral_price = ray(collateral_price)  # / 10**27


                elif function == 'redo':

                    auction_id = args[0]['value']
                    keeper = args[1]['value']

                    log = None

                    if status == 1:

                        for log_block, log_tx_hash, log_function, log_auction_id, log_usr, log_keeper, log_data in logs:
                            if (
                                int(log_auction_id, 16) == int(args[0]['value'])
                                and log_tx_hash.lower() == tx_hash.lower()
                                and log_block == int(block)
                                and log_function == '0x275de7ecdd375b5e8049319f8b350686131c219dd4dc450a08e9cf83b03c865f'
                            ):
                                log = dict(
                                    id=int(log_auction_id, 16),
                                    lot=int(log_data[130:194], 16),
                                    tab=int(log_data[66:130], 16),
                                    top=int(log_data[2:66], 16),
                                    kpr=log_keeper,
                                    coin=int(log_data[194:258], 16),
                                    usr=log_usr
                                )
                                break

                    if log:

                        available_collateral = wad(log['lot'])  # / 10**18
                        debt = rad(log['tab'])  # / 10**45
                        init_price = ray(log['top'])  # / 10**27
                        keeper = log['kpr']
                        incentives = rad(log['coin'])  # / 10**45
                        urn = log['usr']

                        initial_auction_data = init_prices_dict[ilk][auction_id]
                        initial_auction_price = initial_auction_data[1]
                        seconds_passed = timestamp - initial_auction_data[0]
                        c = get_calc(ccc[to_address]['calc'])
                        collateral_price = c.functions.price(
                            initial_auction_price, seconds_passed.seconds
                        ).call()
                        collateral_price = ray(collateral_price)  # / 10**27

                else:

                    pass

            except Exception as e:
                print(e)
                print('#ERR')
                print([load_id, block, timestamp, breadcrumb, tx_hash, tx_index, type, value, from_address, to_address, function, call_arguments, call_outputs, error, status, gas_used])

            try:
                # load_id, block, timestamp, breadcrumb, tx_hash, tx_index, type, value, from_address, to_address, function, call_arguments, call_outputs, error, status, gas_used
                actions.append(
                    [
                        load_id,
                        auction_id,
                        timestamp,
                        block,
                        tx_hash,
                        function,
                        from_address,
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
                        gas_used,
                        status,
                        error,
                        None,
                        breadcrumb,
                        ilk,
                        mkt_price
                    ]
                )
            except Exception as e:
                print(e)
                print([load_id, block, timestamp, breadcrumb, tx_hash, tx_index, type, value, from_address, to_address, function, call_arguments, call_outputs, error, status, gas_used])


    if len(actions) > 0:

        pattern = _write_to_stage(sf, actions, f"{setup['STAGING']}")
        if pattern:
            _write_actions_to_table(
                sf,
                f"{setup['STAGING']}",
                f"{setup['DB']}.INTERNAL.ACTION",
                pattern,
            )
            _clear_stage(sf, f"{setup['STAGING']}", pattern)

    print('{} rows loaded.'.format(len(actions)))

    return


def clips_into_db(**setup):

    last_day = sf.execute(f"""
        select max(date(timestamp))
        from {setup['DB']}.internal.action;
    """).fetchone()[0]
    if not last_day:
        last_day = datetime(2021,4,26).date()

    d = sf.execute(f"""
        select max(date(timestamp))
        from {setup['DB']}.staging.clip
        where timestamp <= '{setup['end_time']}'
    """).fetchone()[0]

    range_to_compute = []
    while last_day < d:
        
        range_from = last_day
        range_to = last_day + timedelta(days=30)

        last_day = range_to

        # print(range_from, range_to)
        range_to_compute.append([range_from, range_to])

    range_to_compute.append([range_to, d])

    for from_date, to_date in range_to_compute:
        # compute
        from_block = sf.execute(f"""
            select max(block)
            from {setup['DB']}.internal.action
        """).fetchone()[0]
        if not from_block:
            from_block = 12317309

        to_block = sf.execute(f"""
            select max(block)
            from {setup['DB']}.staging.clip
            where date(timestamp) <= '{to_date}';
        """).fetchone()[0]

        setup['start_block'] = from_block +1
        setup['end_block'] = to_block
        setup['start_time'] = from_date
        setup['end_time'] = to_date

        get_clipper_actions(**setup)

    return
