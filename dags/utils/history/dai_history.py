"""
Updates Dai Transfer History
"""


def update_dai_history(sf):
    query = """insert overwrite into MAKER.HISTORY.DAI_TRANSFERS (
        select block, timestamp, tx_hash, 'DAI' as token,
            concat('0x', right(topic1, 40)) as sender,
            concat('0x', right(topic2, 40)) as receiver,
            mcd.public.sysadmin_hextoint(log_data) as amount
        from edw_share.raw.events
        where contract = '0x6b175474e89094c44da98b954eedeac495271d0f' and
            topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' and
            status
        order by block, log_index);"""

    try:
        sf.execute(query)
    except Exception as e:
        print(e)
        return dict(status="failure", data="Backend error: %s" % e)
