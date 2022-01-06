import json
import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf


def _fetch_rates(new_blocks, new_vat, **setup):

    rates = dict()
    for row in sf.execute(
        f"""
        SELECT distinct ilk, last_value(rate) over (partition by ilk order by block)
        FROM {setup['db']}.internal.rates; """
    ):
        rates[row[0]] = row[1]

    blocks = []

    for load_id, block, timestamp, block_hash, miner, difficulty, size, extra_data, gas_limit, gas_used, tx_count in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11 
        from @mcd.staging.vaults_extracts/{new_blocks} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():

        block = int(block)

        if block > setup['start_block']:
            blocks.append([block, timestamp])

    vat_operations = []
    for load_id, block, timestamp, breadcrumb, tx_hash, tx_index, type, value, from_address, to_address, function, arguments, outputs, error, status, gas_used in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16   
        from @mcd.staging.vaults_extracts/{new_vat} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():

        block = int(block)
        tx_index = int(tx_index)
        arguments = json.loads(arguments.replace("\'", "\""))
        outputs = json.loads(outputs.replace("\'", "\""))
        status = int(status)

        if status == 1 and function == 'init':
            vat_operations.append(
                [block, tx_index, breadcrumb, tx_hash, timestamp, arguments[0]['value'], 1000000000000000000000000000, function]
            )
        elif status == 1 and function == 'fold':
            vat_operations.append([block, tx_index, breadcrumb, tx_hash, timestamp, arguments[0]['value'], arguments[2]['value'], function])
        else:
            pass

    operations = []
    for i in vat_operations:
        operations.append([i[0], i[1], i[5], i[6]])

    records = []
    pointer = 0
    for block, timestamp in blocks:
        while pointer < len(operations) and operations[pointer][0] == block:
            if operations[pointer][2] not in rates:
                rates[operations[pointer][2]] = 0

            rates[operations[pointer][2]] += int(operations[pointer][3])
            pointer += 1
        for ilk in rates:
            records.append((setup['load_id'], block, timestamp, ilk, rates[ilk]))

    print(f"""Rates: {len(operations)} read, {len(records)} prepare to write""")
    
    pattern = None
    if records:
        pattern = _write_to_stage(sf, records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
