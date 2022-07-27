from dags.connectors.sf import sf, _clear_stage, _write_to_stage, _write_to_table


def load_flops(**setup):
        
    floppers = sf.execute(f"""
        select block, timestamp, tx_hash, concat('0x', lpad(ltrim(return_value, '0x'), 40, '0')) as flopper, to_address
        from edw_share.raw.calls
        where to_address = '0x920ff284ce06eef00082acb1e12617188c928f99'
        and left(call_data, 10) = '0xdc1bf59c'
        and status
        and block > {setup['start_block']}
        and block <= {setup['end_block']};
    """).fetchall()

    # parameters
    # block, timestamp, tx_hash, source, parameter, ilk, from_value, to_value, source_type
    load_flopper_params = list()

    beg = 0.05 # 5% minimum bid increase
    ttl = 10800 # 3 hours bid duration [seconds]
    tau = 172800 # 2 days total auction length [seconds]
    pad = 0.5 # 50% lot increase for tick

    for block, timestamp, tx_hash, flopper, to_address in floppers:

        load_flopper_params.append([block, timestamp, tx_hash, to_address, 'FLOPPER.beg', None, 0, beg, 'FlopFab'])
        load_flopper_params.append([block, timestamp, tx_hash, to_address, 'FLOPPER.ttl', None, 0, ttl, 'FlopFab'])
        load_flopper_params.append([block, timestamp, tx_hash, to_address, 'FLOPPER.tau', None, 0, tau, 'FlopFab'])
        load_flopper_params.append([block, timestamp, tx_hash, to_address, 'FLOPPER.pad', None, 0, pad, 'FlopFab'])

        sf.execute(f"""
            INSERT INTO MAKER.INTERNAL.FLOPPERS(BLOCK, TIMESTAMP, TX_HASH, ADDRESS)
            VALUES({block}, '{timestamp.__str__()[:19]}', '{tx_hash}', '{flopper}');
        """)

    if floppers:
        pattern = _write_to_stage(sf, load_flopper_params, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"{setup['target_db'].split('.')[0]}.{setup['target_db'].split('.')[1]}.{setup['target_db'].split('.')[2]}",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)

    return