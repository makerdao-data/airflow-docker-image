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

from datetime import datetime, timedelta
from web3 import Web3
import json
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, sf_dict, _write_to_stage, _write_to_table, _write_barks_to_table, _clear_stage
from dags.utils.mcd_units import wad, rad, ray
from dags.utils.general import breadcrumb


def get_Bark(bbb, tx_hash, urn, ilk, auction_id):

    details = json.loads(bbb[tx_hash][urn][auction_id]['raw_parameters'])
    Bark_ink = wad(int(details[2]['raw'],16)) # ink
    Bark_due = rad(int(details[4]['raw'],16)) # due

    return Bark_ink, Bark_due


def get_barks(**setup):

    # get bark events

    bark_events = sf.execute(f"""
        select block, timestamp, tx_hash, call_id, raw_parameters::varchar, order_index
        from edw_share.decoded.events
        where event_name = 'Bark'
            and contract_address = lower('0x135954d155898D42C90D2a57824C690e0c7BEf1B')
            and block >= {setup['start_block']}
            and block <= {setup['end_block']}
        order by block;
    """).fetchall()

    b = list()

    for block, timestamp, tx_hash, call_id, raw_parameters, order_index in bark_events:

        b.append([
            block,
            timestamp,
            tx_hash,
            breadcrumb(call_id),
            raw_parameters,
            order_index
        ])

    pattern = None
    if b:
        pattern = _write_to_stage(sf, b, f"{setup['STAGING']}")

        _write_to_table(
            sf,
            f"{setup['STAGING']}",
            f"{setup['DB']}.STAGING.BARK",
            pattern,
        )
        _clear_stage(sf, f"{setup['STAGING']}", pattern)

    all_v = sf.execute("""
        select vault, urn, ilk, ratio, owner
        from mcd.public.current_vaults;
    """).fetchall()

    vvv = dict()
    for vault, urn, ilk, ratio, owner in all_v:

        vvv.setdefault(urn, {})
        vvv[urn] = dict(
            vault=vault,
            ilk=ilk,
            ratio=ratio,
            owner=owner
        )

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

    all_b = sf.execute(f"""
        select 
            tx_hash,
            concat('0x', lpad(ltrim(raw_parameters[1]['raw'], '0x'), 40, '0')) as urn,
            maker.public.etl_hextostr(raw_parameters[0]['raw']) as ilk,
            maker.public.etl_hextoint(raw_parameters[6]['raw']) as auction_id,
            raw_parameters::varchar
        from {setup['DB']}.staging.bark
        where block >= {setup['start_block']}
        and block <= {setup['end_block']};
    """).fetchall()

    bbb = dict()
    for tx, urn, ilk, auction_id, raw_parameters in all_b:

        bbb.setdefault(tx, {})
        bbb[tx].setdefault(urn, {})
        bbb[tx][urn][auction_id] = dict(
            raw_parameters=raw_parameters
        )

    if setup['start_block']:
        barks = sf.execute(f"""
            select block, timestamp, tx_hash, from_address, arguments, outputs, error, status, gas_used
            from {setup['DB']}.staging.dog
            where function = 'bark'
                and block >= {setup['start_block']}
                and block <= {setup['end_block']}
                and status = 1
            order by block, breadcrumb;
        """).fetchall()
    else:
        barks = sf.execute(f"""
            select block, timestamp, tx_hash, from_address, arguments, outputs, error, status, gas_used
            from {setup['DB']}.staging.dog
            where function = 'bark'
                and status = 1
            order by block, breadcrumb;
        """).fetchall()

    brk = []
    for block, timestamp, tx_hash, from_address, arguments, outputs, error, status, gas_used in barks:

        args = json.loads(arguments)
        out = json.loads(outputs)
        auction_id = None
        if out:
            auction_id = out[0]['value']

        try:
            ilk = Web3.toText(args[0]['value']).rstrip('\x00')
        except Exception as e:
            print(e)
            ilk = None

        urn = args[1]['value']

        # vault_info = get_vault_info(urn, ilk)
        collateral = None
        debt = None
        if status == 1:
            collateral, debt = get_Bark(bbb, tx_hash, urn, ilk, auction_id)

        vault_id = vvv[urn]['vault']
        owner = vvv[urn]['owner']
        liq_ratio = vvv[urn]['ratio']
        
        if ilk in ['TUSD-A', 'USDT-A', 'PAXUSD-A', 'USDC-B', 'USDC-A', 'PSM-USDC-A', 'PSM-PAX-A']:
            osm_price = 1
            mkt_price = 1
        else:
            osm_price = ppp[ilk.split('-')[0]][block]['price']
            mkt_price = ppp[ilk.split('-')[0]][block]['price']

        brk.append(
            [
                setup['load_id'],
                block,
                timestamp,
                tx_hash,
                urn,
                vault_id,
                ilk,
                owner,
                collateral,
                debt,
                0.13, # liquidation penalty
                liq_ratio,
                osm_price,
                mkt_price,
                auction_id,
                from_address, # caller
                args[2]['value'], # keeper
                gas_used,
                status,
                error,
            ]
        )

    if len(brk) > 0:
        pattern = _write_to_stage(sf, brk, f"{setup['STAGING']}")
        if pattern:
            _write_barks_to_table(
                sf,
                f"{setup['STAGING']}",
                f"{setup['DB']}.INTERNAL.BARK",
                pattern,
            )
            _clear_stage(sf, f"{setup['STAGING']}", pattern)

    print('{} rows loaded'.format(len(brk)))

    return True


def barks_into_db(**setup):

    last_day = sf.execute(f"""
        select max(date(timestamp))
        from {setup['DB']}.internal.bark
    """).fetchone()[0]
    if not last_day:
        last_day = datetime(2021,4,26).date()

    d = sf.execute(f"""
        select max(date(timestamp))
        from {setup['DB']}.staging.dog
        where timestamp <= '{setup['end_time']}'
    """).fetchone()[0]

    range_to_compute = []
    while last_day < d:
        
        range_from = last_day
        range_to = last_day + timedelta(days=30)

        last_day = range_to

        range_to_compute.append([range_from, range_to])
    range_to_compute.append([range_to, d])

    for from_date, to_date in range_to_compute:
        
        # compute
        from_block = sf.execute(f"""
            SELECT MAX(block)
            FROM {setup['DB']}.INTERNAL.BARK
        """).fetchone()[0]
        if not from_block:
            from_block = 12317309

        to_block = sf.execute(f"""
            SELECT MAX(block)
            FROM {setup['DB']}.STAGING.DOG
            WHERE DATE(timestamp) <= '{to_date}';
        """).fetchone()[0]
        
        setup['start_block'] = from_block +1
        setup['end_block'] = to_block
        setup['start_time'] = from_date
        setup['end_time'] = to_date

        get_barks(**setup)

    return
