"""
Updates MKR Transfer History
"""
from airflow.exceptions import AirflowFailException


def update_mkr_history(sf):

    top_block = sf.execute(
        "select max(BLOCK) from MAKER.HISTORY.MKR_TRANSFERS").fetchone()

    if top_block:
        top_block = top_block[0]
    else:
        top_block = 8928673

    insert_query = f"""insert into MAKER.HISTORY.MKR_TRANSFERS (
        select block, timestamp, tx_hash, 'MKR' as token,
            concat('0x', right(topic1, 40)) as sender,
            concat('0x', right(topic2, 40)) as receiver,
            maker.public.etl_hextoint(log_data) / power(10,18) as amount
        from edw_share.raw.events
        where contract = lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') and
            topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' and
            block > {top_block} and
            status
        order by block, log_index);"""

    try:
        sf.execute(insert_query)
    except Exception as e:
        raise AirflowFailException(e)
