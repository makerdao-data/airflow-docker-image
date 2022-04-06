import json


def _fetch_prices(sf, start_block, end_block):

    prices = sf.execute("""
        select price
        from maker.risk.vaults
        where id = 1;
    """).fetchone()

    if prices in (None, (None,)):
        p = dict()
        p.setdefault('last_scanned_block', end_block)
        p.setdefault('prices', {})
        sf.execute(f"""UPDATE MAKER.RISK.VAULTS
            SET PRICE = PARSE_JSON($${p}$$)
            WHERE ID = 1;
        """)
    else:
        p = json.loads(prices[0])

    pips = sf.execute("""
        SELECT PIP
        FROM MAKER.RISK.VAULTS
        WHERE ID = 1;
    """).fetchone()

    if pips in (None, (None,)):
        print('NO PIPS')
    else:
        pips = json.loads(pips[0])

    for hex_ilk in pips['pips']:

        prices = sf.execute(f"""
            SELECT CASE location
            WHEN '3' THEN 'curr'
            WHEN '4' THEN 'nxt'
            END asset,
            maker.public.etl_hextoint(concat('0x', substr(curr_value, 5))) / power(10,18) as value
            FROM edw_share.raw.storage_diffs
            WHERE lower(contract) = lower('{pips['pips'][hex_ilk]}') AND
            location IN ('3', '4') AND
            block > {start_block} AND
            block <= {end_block} AND
            status
            ORDER BY block, order_index;
        """).fetchall()

        for asset, value in prices:
            
            if asset == 'curr':
                if hex_ilk not in p['prices']:
                    p['prices'].setdefault(hex_ilk, dict(curr=value))
                else:
                    p['prices'][hex_ilk]['curr'] = value
            elif asset == 'nxt':
                if hex_ilk not in p['prices']:
                    p['prices'].setdefault(hex_ilk, dict(nxt=value))
                else:
                    p['prices'][hex_ilk]['nxt'] = value
            else:
                pass
    
    p['last_scanned_block'] = end_block
    sf.execute(f"""
        UPDATE MAKER.RISK.VAULTS
        SET PRICE = PARSE_JSON($${p}$$)
        WHERE ID = 1;
    """)

    return