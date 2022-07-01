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
from connectors.sf import sf, _write_to_stage, _write_to_table, _clear_stage


def get_clipper_calls(**setup):

    currentdir = os.path.dirname(os.path.realpath(__file__))
    parentdir = os.path.dirname(currentdir)
    gr_parentdir = os.path.dirname(parentdir)
    path = os.path.join(gr_parentdir, 'connectors/abis/')
    with open(path + 'clipper.json', 'r') as f:
        abi = json.load(f)

    if setup['start_block'] > setup['end_block']:

        clipper_calls = []
        
    else:

        c = sf.execute(f"""
            select lower(clip)
            from {setup['DB']}.internal.clipper;
        """).fetchall()

        clippers = []
        for clip in c:
            clippers.append(clip)

        clipper_calls = decode_calls(
            tuple(clippers), abi, setup['load_id'], setup['start_block'], setup['end_block'], setup['start_time'], setup['end_time']
        )

        if len(clipper_calls) > 0:
            
            pattern = _write_to_stage(sf, clipper_calls, f"{setup['STAGING']}")
            if pattern:
                _write_to_table(
                    sf,
                    f"{setup['STAGING']}",
                    f"{setup['DB']}.STAGING.CLIP",
                    pattern,
                )
                _clear_stage(sf, f"{setup['STAGING']}", pattern)

        print(f'{len(clipper_calls)} rows loaded')

    return {'calls': len(clipper_calls)}
