from dags.connectors.sf import sf, _clear_stage, _write_to_stage, _write_to_table


def load_clips(**setup):
        
    sf.execute(
        """
            create or replace function maker.public.cf_etl_hextostr (s string)
            returns string language JAVASCRIPT
            as
            'function hexToDec(c) {
            let charCode = c.charCodeAt(0);
            return charCode <= 57 ? charCode - 48 : charCode - 97 + 10;
            }
            let str = "";
            for (let i = 2; i < S.length; i+=2) {
            const byte = hexToDec(S[i])*16 + hexToDec(S[i+1]);
            if (byte > 0) {
            str += String.fromCharCode(byte);
            }
            }
            return str;';
        """
    )

    clippers = sf.execute(f"""
        select distinct maker.public.cf_etl_hextostr(substr(sd.location, 3, 42)) as ilk,
        concat('0x', lpad(ltrim(sd.curr_value, '0x'), 40, '0')) as address,
        sd.block,
        sd.timestamp,
        concat('0x', lpad(ltrim(sd.tx_hash, '0x'), 64, '0')) as tx_hash,
        concat('0x', lpad(ltrim(txs.to_address, '0x'), 40, '0')) as DssSpell
        from edw_share.raw.storage_diffs sd, edw_share.raw.transactions txs
        where sd.tx_hash = txs.tx_hash and
        sd.contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
        sd.location like '1[%' and
        substr(sd.location, length(sd.location)) = '0' and
        sd.status and
        sd.block > {setup['start_block']} and
        sd.block <= {setup['end_block']};
    """).fetchall()

    # parameters
    # block, timestamp, tx_hash, source, parameter, ilk, from_value, to_value, source_type
    load_clippers_params = list()

    # clipper
    # ilk, address, block, tx_hash, dssspell
    load_clippers = list()

    for ilk, address, block, timestamp, tx_hash, DssSpell in clippers:

        load_clippers.append([ilk, address, block, tx_hash, DssSpell])
        load_clippers_params.append([block, timestamp, tx_hash, DssSpell, 'CLIPPER.buf', ilk, 0, 1, 'DssSpell'])

    if load_clippers:
        pattern = _write_to_stage(sf, load_clippers, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"MAKER.INTERNAL.CLIPPERS",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)
        
        pattern = _write_to_stage(sf, load_clippers_params, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"{setup['target_db'].split('.')[0]}.{setup['target_db'].split('.')[1]}.{setup['target_db'].split('.')[2]}",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)
    
    return