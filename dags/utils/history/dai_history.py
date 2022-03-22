"""
Updates Dai Transfer History
"""


def update_dai_history(sf):
    
    top_block = sf.execute(
        "select max(BLOCK) from MAKER.HISTORY.DAI_TRANSFERS").fetchone()
    
    if top_block:
        top_block = top_block[0]
    else:
        top_block = 8928673

    insert_query = f"""insert into MAKER.HISTORY.DAI_TRANSFERS (
        select block, timestamp, tx_hash, 'DAI' as token,
            concat('0x', right(topic1, 40)) as sender,
            concat('0x', right(topic2, 40)) as receiver,
            maker.public.etl_hextoint(log_data) as amount
        from edw_share.raw.events
        where contract = '0x6b175474e89094c44da98b954eedeac495271d0f' and
            topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' and
            block > {top_block} and
            status
        order by block, log_index);"""

    try:
        sf.execute(insert_query)
    except Exception as e:
        print(e)
        return dict(status="failure", data=f"Backend error: {e}")
