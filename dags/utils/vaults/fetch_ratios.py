import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf
from dags.utils.bq_adapters import extract_spot_calls


def _fetch_ratios(**setup):

    calls = extract_spot_calls(
        setup['start_block'], setup['start_time'][:10], setup['end_block'], setup['end_time'][:10]
    )

    records = []
    for call in calls:

        ilk = bytes.fromhex(call[2][10:74]).decode('utf8').replace('\x00', '')
        mat = int(call[2][138:], 16) / 10 ** 27

        records.append([setup['load_id'], call.block_number, call.block_timestamp.__str__()[:19], ilk, mat])

    print(f"""MAT updates: {len(calls)} read, {len(records)} prepare to write""")
    
    pattern = None
    if records:
        pattern = _write_to_stage(sf, records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
