import json
import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf


def _vat_operations(vat, **setup):

    records = list()
    for (
        load_id,
        block,
        timestamp,
        breadcrumb,
        tx_hash,
        tx_index,
        type,
        value,
        from_address,
        to_address,
        function,
        arguments,
        outputs,
        error,
        status,
        gas_used,
    ) in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16   
        from @mcd.staging.vaults_extracts/{vat} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
        """).fetchall():

        block = int(block)
        status = int(status)
        arguments = json.loads(arguments.replace("\'", "\""))
        outputs = json.loads(outputs.replace("\'", "\""))

        if status == 1:

            if function in ('frob', 'grab'):

                record = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    arguments[1]['value'],  # urn
                    arguments[0]['value'],  # ilk
                    arguments[4]['value'],  # dink
                    1,  # sink
                    arguments[5]['value'],  # dart
                    1,  # sart
                    function,
                ]

                records.append(record)

            elif function == 'fork':

                record = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    arguments[1]['value'],  # urn
                    arguments[0]['value'],  # ilk
                    arguments[3]['value'],  # dink
                    -1,  # sink
                    arguments[4]['value'],  # dart
                    -1,  # sart
                    function,
                ]

                records.append(record)

                record = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    arguments[2]['value'],  # urn
                    arguments[0]['value'],  # ilk
                    arguments[3]['value'],  # dink
                    1,  # sink
                    arguments[4]['value'],  # dart
                    1,  # sart
                    function,
                ]

                records.append(record)

            else:

                pass
    
    pattern = None
    if records:
        pattern = _write_to_stage(sf, records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
