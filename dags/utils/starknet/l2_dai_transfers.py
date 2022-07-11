import json
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _clear_stage, _write_to_stage, _write_to_table, sf
from dags.utils.general import breadcrumb, starkcrumb


def l2_dai_transfers():
        
    max_block = sf.execute("""
        select max(block)
        from starknet_l2.transfers.dai;
    """).fetchone()[0]

    if not max_block:
        max_block = 2104

    l2_starknet_dai_transfers = sf.execute(f"""
        select block_number, timestamp, id, tx_hash, trace_id,
        concat('0x', lpad(ltrim(contract, '0x'), 64, '0')) as contract,
        concat('0x', lpad(ltrim(class_hash, '0x'), 64, '0')) as class_hash,
        concat('0x', lpad(ltrim(parameters[0]['value']::varchar, '0x'), 64, '0')) as sender,
        concat('0x', lpad(ltrim(parameters[1]['value']::varchar, '0x'), 64, '0')) as receiver,
        parameters[2]['value'][0]['value'] as low,
        parameters[2]['value'][1]['value'] as high,
        case
            when parameters[0]['value'] = 0 then 'Mint'
            when parameters[1]['value'] = 0 then 'Burn'
        else 'Transfer'
        end as OPERATION
        from sdw.decoded.events
        where contract = '0xda114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3'
        and name = 'Transfer'
        and block_number > {max_block};
    """).fetchall()


    operations = list()
    for block, timestamp, id, tx_hash, trace_id, contract, class_hash, sender, receiver, low, high, operation in l2_starknet_dai_transfers:

        operations.append([
            block,
            timestamp,
            starkcrumb(id),
            tx_hash,
            breadcrumb(trace_id),
            contract,
            class_hash,
            sender,
            receiver,
            low,
            high,
            operation
        ])

    pattern = None
    if operations:
        pattern = _write_to_stage(sf, operations, f"starknet_l2.transfers.extracts")

        _write_to_table(
            sf,
            f"starknet_l2.transfers.extracts",
            f"starknet_l2.transfers.dai",
            pattern,
        )
        _clear_stage(sf, f"starknet_l2.transfers.extracts", pattern)
