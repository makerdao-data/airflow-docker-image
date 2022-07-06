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
from dags.connectors.sf import sf
from dags.utils.decode import EDWCall
from dags.utils.general import breadcrumb


def edw_decode_calls(
    contract=None, abi=None, load_id=None, start_block=None, end_block=None, start_time=None, end_time=None
):

    calls = edw_extract_calls(start_block, end_block, start_time, end_time, contract, abi)
    decoded_calls = [
        [
            load_id.__str__()[:19],
            call.block_number,
            call.timestamp.__str__()[:19],
            breadcrumb(call.breadcrumb),
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


def edw_extract_calls(start_block=None, end_block=None, start_time=None, end_time=None, contract=None, abi=None):

    if len(contract) == 1:
        c = "('" + contract[0] + "')"
    else:
        c = contract

    q = f"""
        select tx_hash, order_index, call_id, from_address, to_address, call_value, call_data, return_value, gas_used, revert_reason, status, timestamp, block
        from edw_share.raw.calls
        where to_address in {c}
        and block >= {start_block}
        and block <= {end_block}
        and date(timestamp) >= '{start_time[:10]}'
        and date(timestamp) <= '{end_time[:10]}'
        order by block, order_index;
    """

    rows = sf.execute(q).fetchall()

    calls = []
    for row in rows:
        call = EDWCall(row, abi)
        calls.append(call)

    return calls