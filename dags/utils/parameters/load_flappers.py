
# uint256  public   beg = 1.05E18;  // 5% minimum bid increase
# uint48   public   ttl = 3 hours;  // 3 hours bid duration         [seconds]
# uint48   public   tau = 2 days;   // 2 days total auction length  [seconds]

from dags.connectors.sf import sf, _clear_stage, _write_to_stage, _write_to_table


def load_flaps(**setup):
        
    flappers = sf.execute(f"""
        select block, timestamp, tx_hash, to_address
        from edw_share.raw.calls
        where to_address = '0x920ff284ce06eef00082acb1e12617188c928f99'
        and left(call_data, 10) = '0x60806040'
        and status
        and block > {setup['start_block']}
        and block <= {setup['end_block']};
    """).fetchall()

    # parameters
    # block, timestamp, tx_hash, source, parameter, ilk, from_value, to_value, source_type
    load_flapper_params = list()

    beg = 0.05 # 5% minimum bid increase
    ttl = 10800 # 3 hours bid duration [seconds]
    tau = 172800 # 2 days total auction length [seconds]

    for block, timestamp, tx_hash, to_address in flappers:

        load_flapper_params.append([block, timestamp, tx_hash, None, 'FLAPPER.beg', None, 0, beg, None])
        load_flapper_params.append([block, timestamp, tx_hash, None, 'FLAPPER.ttl', None, 0, ttl, None])
        load_flapper_params.append([block, timestamp, tx_hash, None, 'FLAPPER.tau', None, 0, tau, None])

        sf.execute(f"""
            INSERT INTO MAKER.INTERNAL.FLAPPERS(BLOCK, TIMESTAMP, TX_HASH, ADDRESS)
            VALUES({block}, '{timestamp.__str__()[:19]}', '{tx_hash}', '{to_address}');
        """)

    if flappers:
        pattern = _write_to_stage(sf, load_flapper_params, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"{setup['target_db'].split('.')[0]}.{setup['target_db'].split('.')[1]}.{setup['target_db'].split('.')[2]}",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)
    
    return