import json
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _clear_stage, _write_to_stage, _write_to_table, sf
from dags.utils.general import breadcrumb, starkcrumb


def l2_dai_bridge_events():
    max_block = sf.execute("""
        select max(block)
        from starknet_l2.decoded.bridge;
    """).fetchone()[0]

    if not max_block:
        max_block = 2104

    operations = list()

    '''
        DEPOSITS
    '''

    l2_starknet_dai_deposits= sf.execute(f"""
        select e.block_number, e.timestamp, e.id, e.tx_hash, e.trace_id,
        concat('0x', lpad(ltrim(e.contract, '0x'), 64, '0')) as contract,
        concat('0x', lpad(ltrim(e.class_hash, '0x'), 64, '0')) as class_hash,
        concat('0x', lpad(ltrim(e.parameters[0]['value']::varchar, '0x'), 64, '0')) as account,
        e.parameters[1]['value'][0]['value'] as low,
        e.parameters[1]['value'][1]['value'] as high,
        'Deposit' as OPERATION,
        concat('0x', lpad(ltrim(t.inputs[4]['value']::varchar, '0x'), 40, '0')) as L1ADDRESS
        from sdw.decoded.events e
        left join sdw.decoded.transactions t
        on concat('0x', lpad(ltrim(e.tx_hash, '0x'), 64, '0')) = concat('0x', lpad(ltrim(t.tx_hash, '0x'), 64, '0'))
        where e.contract = '0x1108cdbe5d82737b9057590adaf97d34e74b5452f0628161d237746b6fe69e'
        and e.name = 'deposit_handled'
        and e.block_number > {max_block};
    """).fetchall()


    for block, timestamp, id, tx_hash, trace_id, contract, class_hash, account, low, high, operation, l1address in l2_starknet_dai_deposits:

        amount = str(int(low) + (int(high) << 128))

        operations.append([
            block,
            timestamp,
            starkcrumb(id),
            tx_hash,
            breadcrumb(trace_id),
            contract,
            class_hash,
            account,
            low,
            high,
            amount,
            operation,
            l1address
        ])


    '''
        WITHDRAWS
    '''

    l2_starknet_dai_withdraws = sf.execute(f"""
        select block_number, timestamp, id, tx_hash, trace_id,
        concat('0x', lpad(ltrim(contract, '0x'), 64, '0')) as contract,
        concat('0x', lpad(ltrim(class_hash, '0x'), 64, '0')) as class_hash,
        concat('0x', lpad(ltrim(parameters[0]['value']::varchar, '0x'), 40, '0')) as l1recipient,
        parameters[1]['value'][0]['value'] as low,
        parameters[1]['value'][1]['value'] as high,
        concat('0x', lpad(ltrim(parameters[2]['value'], '0x'), 64, '0')) as caller_address,
        'Withdraw'
        from sdw.decoded.events
        where contract = '0x1108cdbe5d82737b9057590adaf97d34e74b5452f0628161d237746b6fe69e'
        and name = 'withdraw_initiated'
        and block_number > {max_block};
    """).fetchall()


    for block, timestamp, id, tx_hash, trace_id, contract, class_hash, l1recipient, low, high, caller_address, operation in l2_starknet_dai_withdraws:

        amount = str(int(low) + (int(high) << 128))

        operations.append([
            block,
            timestamp,
            starkcrumb(id),
            tx_hash,
            breadcrumb(trace_id),
            contract,
            class_hash,
            caller_address,
            low,
            high,
            amount,
            operation,
            l1recipient
        ])

    pattern = None
    if operations:
        pattern = _write_to_stage(sf, operations, f"starknet_l2.transfers.extracts")

        _write_to_table(
            sf,
            f"starknet_l2.transfers.extracts",
            f"starknet_l2.decoded.bridge",
            pattern,
        )
        _clear_stage(sf, f"starknet_l2.transfers.extracts", pattern)

    return 