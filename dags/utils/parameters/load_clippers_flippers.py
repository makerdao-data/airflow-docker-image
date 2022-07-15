from dags.connectors.sf import sf


def load_clips_flips(**setup):
        
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

    sf.execute(f"""
        insert into maker.internal.clippers
        select distinct maker.public.cf_etl_hextostr(substr(sd.location, 3, 42)) as ilk,
        concat('0x', lpad(ltrim(sd.curr_value, '0x'), 40, '0')) as address,
        sd.block,
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
    """)

    sf.execute(f"""
        insert into maker.internal.flippers
        select distinct block, concat('0x', lpad(ltrim(tx_hash, '0x'), 64, '0')) as tx_hash,
        maker.public.cf_etl_hextostr(substr(location, 3, 42)) as ilk, concat('0x', lpad(ltrim(curr_value, '0x'), 40, '0')) as address
        from edw_share.raw.storage_diffs
        where contract = lower('0xa5679C04fc3d9d8b0AaB1F0ab83555b301cA70Ea') and
            location like '1[%' and
            substr(location, length(location)) = '0'and
        status and
        block > {setup['start_block']} and
        block <= {setup['end_block']};
    """)

    return