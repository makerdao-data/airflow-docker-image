import sys
sys.path.append('/opt/airflow/')
from dags.utils.bq_adapters import extract_bq_blocks
from dags.connectors.sf import _write_to_stage, sf
from decimal import Decimal


def _fetch_blocks(**setup):

    blocks = extract_bq_blocks(
        setup['start_block'] + 1, setup['start_time'].__str__()[:10], setup['end_block'], setup['end_time']
    )
    records = [
        [
            setup['load_id'],
            block.block_number,
            block.timestamp.__str__()[:19],
            block.block_hash,
            block.miner,
            block.difficulty,
            block.size,
            block.extra_data,
            block.gas_limit,
            block.gas_used,
            block.tx_count,
        ]
        for block in blocks
    ]

    print(f"""Blocks: {len(blocks)} read, {len(records)} prepared to write""")
    
    pattern = None
    if records:
        pattern = _write_to_stage(sf, records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
