"""
Updates DAI Transfer History
"""
from airflow.exceptions import AirflowFailException


def update_dai_history(sf):

    block_cap = sf.execute(
        "select max(BLOCK) from EDW_SHARE.RAW.EVENTS;"
    ).fetchone()

    top_block = sf.execute(
        "select max(BLOCK) from MAKER.TRANSFERS.DAI;"
    ).fetchone()

    if top_block == (None,):
        top_block = None

    if top_block:
        top_block = top_block[0]
    else:
        # If the table with transfers is empty, load the transfers from the very first Mint
        top_block = 8928673

    # # Layout of the MAKER.TRANSFERS.DAI table
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

    # Transfers
    sf.execute(f"""
        insert into maker.transfers.dai (
        select block, timestamp, tx_hash, call_id, 'DAI' as token,
            case when topic1 = '0x0' then 'Mint'
            when topic2 = '0x0' then 'Burn'
            else 'Transfer' end as OPERATION,
            concat('0x', lpad(ltrim(topic1, '0x'), 40, '0')) as SENDER,
            concat('0x', lpad(ltrim(topic2, '0x'), 40, '0')) as RECEIVER,
            maker.public.big_int(log_data)::integer as raw_amount,
            maker.public.big_int(log_data)::integer / power(10,18) as amount,
            order_index
        from edw_share.raw.events
        where contract = '0x6b175474e89094c44da98b954eedeac495271d0f' and
            topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' and
            block > {top_block} and
            block <= {block_cap[0]} and
            status);"""
    )

    return