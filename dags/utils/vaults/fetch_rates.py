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
    for b in new_blocks:
        if b[1] > setup['start_block']:
            blocks.append([b[1], b[2]])

    vat_operations = []
    for i in new_vat:
        if i[14] == 1 and i[10] == 'init':
            vat_operations.append(
                [i[1], i[5], i[3], i[4], i[2], i[11][0]['value'], 1000000000000000000000000000, i[10]]
            )
        elif i[14] == 1 and i[10] == 'fold':
            vat_operations.append([i[1], i[5], i[3], i[4], i[2], i[11][0]['value'], i[11][2]['value'], i[10]])
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
