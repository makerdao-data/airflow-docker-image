import json


def _fetch_mats(sf, start_block, end_block):
        
    mats = sf.execute("""
        SELECT mat
        FROM maker.risk.vaults
        WHERE id = 1;
    """).fetchone()


    if mats in (None, (None,)):
        m = dict()
        m.setdefault('last_scanned_block', end_block)
        m.setdefault('mats', {})
        sf.execute(f"""
            UPDATE MAKER.RISK.VAULTS
            SET MAT = PARSE_JSON($${m}$$)
            WHERE ID = 1;
        """)
    else:
        m = json.loads(mats[0])

    mats = sf.execute(f"""
        SELECT substr(location, 3, 66) as hex_ilk,
        'mat' as asset,
        maker.public.etl_hextoint(curr_value) / power(10,27) as value
        FROM edw_share.raw.storage_diffs
        WHERE lower(contract) = lower('0x65C79fcB50Ca1594B025960e539eD7A9a6D434A3') AND
        location LIKE '1[0x%].1' AND
        block > {start_block} AND
        block <= {end_block} AND
        status
        ORDER BY block, order_index;
    """).fetchall()

    for hex_ilk, asset, value in mats:

        if hex_ilk not in m['mats']:
            m['mats'].setdefault(hex_ilk, round(value, 2))
        else:
            m['mats'][hex_ilk] = round(value, 2)

    m['last_scanned_block'] = end_block
    output = sf.execute(f"""
        UPDATE MAKER.RISK.VAULTS
        SET MAT = PARSE_JSON($${m}$$)
        WHERE ID = 1;
    """)
