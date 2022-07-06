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

from datetime import datetime
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.gcp import bq_query
from dags.utils.decode import GasCall, Call, Block
from dags.utils.general import breadcrumb


def extract_calls(start_block=None, end_block=None, start_time=None, end_time=None, contract=None, abi=None):

    if len(contract) == 1:
        c = "('" + contract[0] + "')"
    else:
        c = contract

    query_calls = f"""
        SELECT DISTINCT *, (SELECT STRING_AGG(FORMAT('%03d', CAST(IF(traces='', '0', traces) AS INT64)), '_') FROM UNNEST(SPLIT(trace_address)) AS traces) as breadcrumb
        FROM `bigquery-public-data.crypto_ethereum.traces`
        WHERE (block_number between {start_block} and {end_block})
            and date(block_timestamp) >= '{start_time[:10]}'
            and date(block_timestamp) <= '{end_time[:10]}'
            and to_address in {c}
        ORDER BY block_number, transaction_index, breadcrumb; """

    rows = bq_query(query_calls)

    calls = []
    for row in rows:
        call = Call(row, abi)
        calls.append(call)

    return calls


def extract_pip_events(start_block, start_time, end_block, end_time, oracles, signatures):

    signatures_list = ','.join(["'%s'" % signature for signature in signatures])
    oracles_list = ','.join(["'%s'" % oracle.lower() for oracle in oracles])

    query_calls = f"""
        SELECT *
        FROM `bigquery-public-data.crypto_ethereum.logs`
        WHERE
            (block_timestamp between '{start_time}' and '{end_time}')
            AND (block_number between {start_block} and {end_block})
            AND address in ({oracles_list})
            AND array_length(topics) = 1
            AND topics[offset(0)] in ({signatures_list})
        ORDER BY
            block_number,
            transaction_index,
            log_index"""

    rows = bq_query(query_calls)

    events = []
    for row in rows:
        for token in oracles[row.address]:
            if row.topics[0] == signatures[0]:
                event = dict(
                    block=row.block_number,
                    times=row.block_timestamp,
                    ilk=token,
                    price=int(row.data, 16) / 10 ** 18,
                )
            else:
                event = dict(
                    block=row.block_number,
                    times=row.block_timestamp,
                    ilk=token,
                    price=int(row.data[:66], 16) / 10 ** 18,
                )
            events.append(event)

    return events


def extract_bq_blocks(start_block=None, start_date=None, safe_last_block=None, safe_last_time=None):

    if type(safe_last_time) == int:
        safe_time_dt = datetime.fromtimestamp(safe_last_time)
        safe_last_time = datetime.strftime(safe_time_dt, '%Y-%m-%d %H:%M:%S')
    else:
        pass

    query_calls = f"""
        SELECT number, timestamp, `hash`, miner, difficulty, size, extra_data,
            gas_limit, gas_used, transaction_count
        FROM `bigquery-public-data.crypto_ethereum.blocks`
        WHERE
            number >= {start_block}
            AND number <= {safe_last_block}
            AND date(timestamp) >= '{start_date}'
            AND timestamp <= '{safe_last_time}'
        ORDER BY number"""

    rows = bq_query(query_calls)

    blocks = []

    for row in rows:
        block = Block(row)
        blocks.append(block)

    return blocks


def decode_calls(
    contract=None, abi=None, load_id=None, start_block=None, end_block=None, start_time=None, end_time=None
):

    calls = extract_calls(start_block, end_block, start_time, end_time, contract, abi)
    decoded_calls = [
        [
            load_id.__str__()[:19],
            call.block_number,
            call.timestamp.__str__()[:19],
            call.breadcrumb,
            call.tx_hash,
            call.tx_index,
            call.type,
            str(call.value),
            call.from_address,
            call.to_address,
            call.function,
            call.arguments,
            call.outputs,
            call.error,
            call.status,
            call.gas_used,
        ]
        for call in calls
    ]

    return decoded_calls


def extract_spot_calls(start_block, start_date, end_block, end_date):

    query = f"""
        SELECT block_number, block_timestamp, input
        FROM bigquery-public-data.crypto_ethereum.traces
        WHERE
            block_number > {start_block} AND date(block_timestamp) >= '{start_date}'
            AND block_number <= {end_block} AND date(block_timestamp) <= '{end_date}'
            AND to_address = '0x65c79fcb50ca1594b025960e539ed7a9a6d434a3'
            AND substr(input, 1, 10) = '0x1a0b287e' AND status = 1; """

    rows = bq_query(query)

    return rows


def extract_calls_gas(
    start_block=None, end_block=None, start_time=None, end_time=None, contract=None, abi=None
):

    if len(contract) == 1:
        c = "('" + contract[0] + "')"
    else:
        c = contract

    query_calls = f"""
        SELECT DISTINCT
            tr.transaction_hash, tr.transaction_index, tr.from_address, tr.to_address, tr.value,
            tr.input, tr.output, tr.trace_type, tr.call_type, tr.reward_type, tr.gas, tr.gas_used,
            tr.subtraces, tr.trace_address, tr.error, tr.status, tr.block_timestamp, tr.block_number,
            tr.block_hash, tr.trace_id, (SELECT STRING_AGG(FORMAT('%03d', CAST(IF(traces='', '0', traces) AS INT64)), '_') FROM UNNEST(SPLIT(tr.trace_address)) AS traces) as breadcrumb,
            tx.gas_price
        FROM `bigquery-public-data.crypto_ethereum.traces` tr
        JOIN `bigquery-public-data.crypto_ethereum.transactions` tx
        ON tr.transaction_hash = tx.`hash`
        WHERE (tr.block_number between {start_block} and {end_block})
            and date(tr.block_timestamp) >= '{start_time[:10]}'
            and date(tr.block_timestamp) <= '{end_time[:10]}'
            and tr.to_address in {c}
            and (tx.block_number between {start_block} and {end_block})
            and date(tx.block_timestamp) >= '{start_time[:10]}'
            and date(tx.block_timestamp) <= '{end_time[:10]}'
        ORDER BY tr.block_number, tr.transaction_index, breadcrumb;
    """

    rows = bq_query(query_calls)

    calls = []
    for row in rows:
        call = GasCall(row, abi)
        calls.append(call)

    return calls
