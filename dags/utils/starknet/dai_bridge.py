import json
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _clear_stage, _write_to_stage, _write_to_table, sf
from dags.utils.starknet.bridge.tooling import *
from dags.utils.general import breadcrumb


def l1_dai_bridge_events():
    max_block = sf.execute("""
        select max(block)
        from starknet_l1.decoded.bridge;
    """).fetchone()[0]

    if not max_block:
        max_block = 14751640

    l1_starkner_dai_bridge_events = sf.execute(f"""
        select block, timestamp, tx_hash, log_index, call_id, contract, log_data, topic0, topic1, topic2, topic3, order_index
        from edw_share.raw.events
        where contract = '0x659a00c33263d9254fed382de81349426c795bb6'
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
            f"starknet_l1.decoded.bridge",
            pattern,
        )
        _clear_stage(sf, f"starknet_l1.decoded.extracts", pattern)
