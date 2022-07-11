import json
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _clear_stage, _write_to_stage, _write_to_table, sf
from dags.utils.starknet.escrow.tooling import *
from dags.utils.general import breadcrumb


def starknet_dai_escrow_events():

    max_block = sf.execute("""
        select max(block)
        from starknet_l1.decoded.escrow;
    """).fetchone()[0]

    if not max_block:
        max_block = 14742549

    l1_starkner_dai_bridge_events = sf.execute(f"""
        select block, timestamp, tx_hash, log_index, call_id, contract, log_data, topic0, topic1, topic2, topic3, order_index
        from edw_share.raw.events
        where contract = '0x0437465dfb5b79726e35f08559b0cbea55bb585c'
        and block > {max_block}
        and status;
    """).fetchall()


    decoded = list()
    for block, timestamp, tx_hash, log_index, call_id, contract, log_data, topic0, topic1, topic2, topic3, order_index in l1_starkner_dai_bridge_events:

        e, v = decode_event(topic0, topic1, topic2, topic3, log_data)

        decoded.append([
            block,
            timestamp,
            tx_hash,
            log_index,
            breadcrumb(call_id),
            contract,
            log_data,
            topic0,
            topic1,
            topic2,
            topic3,
            order_index,
            e,
            json.dumps(v)
        ])

    pattern = None
    if decoded:
        pattern = _write_to_stage(sf, decoded, f"starknet_l1.decoded.extracts")

        _write_to_table(
            sf,
            f"starknet_l1.decoded.extracts",
            f"starknet_l1.decoded.escrow",
            pattern,
        )
        _clear_stage(sf, f"starknet_l1.decoded.extracts", pattern)
