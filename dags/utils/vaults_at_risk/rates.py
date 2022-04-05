import json


def _fetch_rates(sf, start_block, end_block):
    
    rates = sf.execute("""
        select rate
        from maker.risk.vaults
        where id = 1;
    """).fetchone()

    if rates in (None, (None,)):
        r = dict()
        r.setdefault('last_scanned_block', end_block)
        r.setdefault('rates', {})
        sf.execute(f"""
            UPDATE MAKER.RISK.VAULTS
            SET RATE = PARSE_JSON($${r}$$)
            WHERE ID = 1;
        """)
    else:
        r = json.loads(rates[0])

    rates = sf.execute(f"""
        select substr(location, 3, 66) as hex_ilk,
        'rate' as asset,
        maker.public.etl_hextoint(curr_value) / power(10,27) as value
        from edw_share.raw.storage_diffs
        where lower(contract) = lower('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B') and
        location like '2[0x%].1' and
        block > {start_block} and block <= {end_block} and
        status
        order by block, order_index;
    """).fetchall()

    for hex_ilk, asset, value in rates:

        if hex_ilk not in r['rates']:
            r['rates'].setdefault(hex_ilk, value)
        else:
            r['rates'][hex_ilk] = value

    r['last_scanned_block'] = end_block
    sf.execute(f"""
        UPDATE MAKER.RISK.VAULTS
        SET RATE = PARSE_JSON($${r}$$)
        WHERE ID = 1;
    """)

    return