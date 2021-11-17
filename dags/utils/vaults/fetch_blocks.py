from datetime import datetime
from dags.utils.bq_adapters import extract_bq_blocks


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
            str(block.difficulty),
            block.size,
            block.extra_data,
            block.gas_limit,
            block.gas_used,
            block.tx_count,
        ]
        for block in blocks
    ]

    print(f"""Blocks: {len(blocks)} read, {len(records)} prepared to write""")

    return records
