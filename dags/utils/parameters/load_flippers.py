from dags.connectors.sf import sf, _clear_stage, _write_to_stage, _write_to_table


def load_flips(**setup):
        
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

    flippers = sf.execute(f"""
        select distinct sd.block, sd.timestamp, concat('0x', lpad(ltrim(sd.tx_hash, '0x'), 64, '0')) as tx_hash,
        maker.public.cf_etl_hextostr(substr(sd.location, 3, 42)) as ilk, concat('0x', lpad(ltrim(sd.curr_value, '0x'), 40, '0')) as address,
        concat('0x', lpad(ltrim(t.to_address, '0x'), 40, '0')) as source
        from edw_share.raw.storage_diffs sd
        join edw_share.raw.transactions t
        on sd.tx_hash = t.tx_hash
        where sd.contract = lower('0xa5679C04fc3d9d8b0AaB1F0ab83555b301cA70Ea') and
            sd.location like '1[%' and
            substr(sd.location, length(sd.location)) = '0'and
        sd.status and
        sd.block > {setup['start_block']} and
        sd.block <= {setup['end_block']};""").fetchall()

    beg = 0.05 # 5% minimum bid increase
    ttl = 10800 # 3 hours bid duration [seconds]
    tau = 172800 # 2 days total auction length [seconds]

    # flippers
    # block, tx_hash, ilk, address
    load_flippers = list()

    # parameters
    # block, timestamp, tx_hash, source, parameter, ilk, from_value, to_value, source_type
    load_flippers_parameters = list()

    for block, timestamp, tx_hash, ilk, address, source in flippers:

        load_flippers.append([block, tx_hash, ilk, address])
        load_flippers_parameters.append([block, timestamp, tx_hash, source, 'FLIPPER.beg', ilk, 0, beg, 'DssSpell'])
        load_flippers_parameters.append([block, timestamp, tx_hash, source, 'FLIPPER.ttl', ilk, 0, ttl, 'DssSpell'])
        load_flippers_parameters.append([block, timestamp, tx_hash, source, 'FLIPPER.tau', ilk, 0, tau, 'DssSpell'])

    if load_flippers:
        pattern = _write_to_stage(sf, load_flippers, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"MAKER.INTERNAL.FLIPPERS",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)
        
        pattern = _write_to_stage(sf, load_flippers_parameters, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"{setup['target_db'].split('.')[0]}.{setup['target_db'].split('.')[1]}.{setup['target_db'].split('.')[2]}",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)


    return