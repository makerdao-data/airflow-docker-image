import json


def _fetch_urns(sf, start_block, end_block):
        
    urns = sf.execute("""
        select urn
        from maker.risk.vaults
        where id = 1;
    """).fetchone()

    if urns in (None, (None,)):
        u = dict()
        u.setdefault('last_scanned_block', end_block)
        u.setdefault('urns', {})
        u.setdefault('hex_ilks', [])
        sf.execute(f"""
            UPDATE MAKER.RISK.VAULTS
            SET URN = PARSE_JSON($${u}$$)
            WHERE ID = 1;
        """)
    else:
        u = json.loads(urns[0])

    # ink & art -- this is also a source of collaterals
    urns = sf.execute(f"""
        select block,
        maker.public.etl_hextostr(substr(location, 3, 66)) as ilk,
        substr(location, 3, 66) as hex_ilk,
        substr(location, 73, 42) as urn,
        case substr(location, length(location))
        when '0' then 'ink'
        when '1' then 'art'
        end asset,
        maker.public.etl_hextoint(curr_value) / power(10,18) as value
        from edw_share.raw.storage_diffs
        where lower(contract) = lower('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B') and
        location like '3[%' and
        block > {start_block} and block <= {end_block} and
        status
        order by block, order_index;
    """).fetchall()

    for block, ilk, hex_ilk, urn, asset, value in urns:
        
        if hex_ilk not in u['hex_ilks']:
            u['hex_ilks'].append(hex_ilk)
        
        if urn not in u['urns']:
            if asset == 'ink':
                u['urns'].setdefault(urn, dict(
                    ilk = ilk,
                    hex_ilk = hex_ilk,
                    ink = value,
                    art = 0,
                ))
            elif asset == 'art':
                u['urns'].setdefault(urn, dict(
                    ilk = ilk,
                    hex_ilk = hex_ilk,
                    ink = 0,
                    art = value,
                ))
            else:
                pass
        else:
            if asset == 'ink':
                u['urns'][urn]['ink'] = value
            elif asset == 'art':
                u['urns'][urn]['art'] = value
            else:
                pass
    
    u['last_scanned_block'] = end_block
    sf.execute(f"""
        UPDATE MAKER.RISK.VAULTS
        SET URN = PARSE_JSON($${u}$$)
        WHERE ID = 1;
    """)

    return