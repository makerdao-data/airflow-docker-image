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

import json
import os, sys

sys.path.append('/opt/airflow/')
from dags.utils.bq_adapters import decode_calls
from dags.connectors.sf import sf, _write_to_stage, _write_to_table, _clear_stage


def get_dog_calls(load_id, start_block, end_block, start_time, end_time, DB, STAGING):

    currentdir = os.path.dirname(os.path.realpath(__file__))
    parentdir = os.path.dirname(currentdir)
    gr_parentdir = os.path.dirname(parentdir)
    path = os.path.join(gr_parentdir, 'connectors/abis/')
    with open(path + 'dog.json', 'r') as f:
        abi = json.load(f)

    if start_block > end_block:

        dog_calls = []
        
    else:
        dog_calls = decode_calls(
            ('0x135954d155898D42C90D2a57824C690e0c7BEf1B'.lower(),),
            abi,
            load_id,
            start_block,
            end_block,
            start_time,
            end_time,
        )

        if len(dog_calls) > 0:
            
            pattern = _write_to_stage(sf, dog_calls, f"{DB}.staging.liquidations_extracts")
            if pattern:
                _write_to_table(
                    sf,
                    f"{DB}.staging.liquidations_extracts",
                    f"{DB}.staging.dog",
                    pattern,
                )
                _clear_stage(sf, f"{DB}.staging.liquidations_extracts", pattern)

        print(f'{len(dog_calls)} rows loaded')

    return {'calls': len(dog_calls)}
