import json


def _fetch_pips(sf, start_block, end_block):

    pips = sf.execute("""
        select pip
        from maker.risk.vaults
        where id = 1;
    """).fetchone()

    if pips in (None, (None,)):
        p = dict()
        p.setdefault('last_scanned_block', end_block)
        p.setdefault('pips', {})
        sf.execute(f"""UPDATE MAKER.RISK.VAULTS
            SET PIP = PARSE_JSON($${p}$$)
            WHERE ID = 1;
        """)
    else:
        p = json.loads(pips[0])

    pips = sf.execute(f"""
        SELECT substr(location, 3, 66) as hex_ilk,
        'pip' as asset,
        curr_value as value
        FROM edw_share.raw.storage_diffs
        WHERE lower(contract) = lower('0x65C79fcB50Ca1594B025960e539eD7A9a6D434A3') AND
        location LIKE '1[0x%].0' AND
        block > {start_block} AND
        block <= {end_block} AND
        status
        ORDER BY block, order_index;
    """).fetchall()


    for hex_ilk, asset, value in pips:

        if hex_ilk not in p['pips']:
            p['pips'].setdefault(hex_ilk, value)
        else:
            p['pips'][hex_ilk] = value

    p['last_scanned_block'] = end_block
    output = sf.execute(f"""
        UPDATE MAKER.RISK.VAULTS
        SET PIP = PARSE_JSON($${p}$$)
        WHERE ID = 1;
    """)

    return