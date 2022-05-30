"""
Updates MKR Transfer History
"""
from airflow.exceptions import AirflowFailException
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf


def update_mkr_history(sf):

    block_cap = sf.execute(
        "select max(BLOCK) from EDW_SHARE.RAW.EVENTS;"
    ).fetchone()

    top_block = sf.execute(
        "select max(BLOCK) from MAKER.TRANSFERS.MKR;"
    ).fetchone()

    if top_block == (None,):
        top_block = None

    if top_block:
        top_block = top_block[0]
    else:
        # If the table with transfers is empty, load the transfers from the very first Mint
        top_block = 4620854

    # # Layout of the MAKER.TRANSFERS.MKR table
    # BLOCK NUMBER(38,0),
	# TIMESTAMP TIMESTAMP_NTZ(9),
	# TX_HASH VARCHAR(66),
    # ORDER_INDEX VARCHAR(128),
	# TOKEN VARCHAR(3),
    # OPERATION VARCHAR(128),
	# SENDER VARCHAR(68),
	# RECEIVER VARCHAR(68),
    # RAW_AMOUNT NUMBER(38, 0),
	# AMOUNT FLOAT,
    # ORDER_ID NUMBER(38,0)

    # Mints
    sf.execute(f"""
        insert into maker.transfers.mkr (
            select block, timestamp, tx_hash, call_id, 'MKR' TOKEN,
                'Mint' OPERATION,
                'Mint' SENDER,
                concat('0x', lpad(ltrim(topic1, '0x'), 40, '0')) as RECEIVER,
                maker.public.big_int(log_data)::integer as raw_amount,
                maker.public.big_int(log_data) / power(10,18) as amount,
                order_index
            from edw_share.raw.events
            where contract = lower('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2') and
                right(topic0, 63) = lower('f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885') and
                block > {top_block} and
                block <= {block_cap[0]} and
                status
        );"""
    )

    # Transfers
    sf.execute(f"""
        insert into maker.transfers.mkr (
            select block, timestamp, tx_hash, call_id, 'MKR' as TOKEN,
                'Transfer' OPERATION,
                concat('0x', lpad(ltrim(topic1, '0x'), 40, '0')) as SENDER,
                concat('0x', lpad(ltrim(topic2, '0x'), 40, '0')) as RECEIVER,
                maker.public.big_int(log_data)::integer as raw_amount,
                maker.public.big_int(log_data) / power(10,18) as amount,
                order_index
            from edw_share.raw.events
            where contract = lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') and
                topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' and
                block > {top_block} and
                block <= {block_cap[0]} and
                status
        );"""
    )

    return