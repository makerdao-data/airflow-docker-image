import pandas as pd

import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, sa
from dags.utils.parameters.load_clippers_flippers import load_clips_flips


def fetch_params(engine, setup) -> pd.DataFrame:
    """
    Function to fetch protocol parameters
    """

    # Create type conversion function in-snowflake
    sf.execute(
        """
            create or replace function maker.public.prot_params_etl_hextostr (s string)
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

    sf.execute(
        """
            create or replace function maker.public.prot_params_etl_hextobigint (s string)
            returns varchar language JAVASCRIPT
            as
            'if (S !== null && S !== "" && S !== "0x") {
            _int = BigInt(S, 16);
            }
            else {
            _int = 0;
            }
            return _int';
        """
    )

    vat_line_dust = pd.read_sql(f"""
            // VAT parameters (line, dust)
            select block, timestamp, tx_hash,
            iff(substr(location, length(location)) = '3', 'VAT.ilks.line', 'VAT.ilks.dust') as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 45) as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b' and
            location like '2[%' and
            substr(location, length(location)) in ('3' ,'4') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    dciam_line_gap = pd.read_sql(f"""
            // DC-IAM parameters (line, gap)
            select block, timestamp, tx_hash,
            case substr(location, length(location))
            when '0' then 'DC-IAM.ilks.line'
            when '1' then 'DC-IAM.ilks.gap'
            end as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 45) as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
            location like '0[%' and
            substr(location, length(location)) in ('0', '1') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    dciam_ttl = pd.read_sql(f"""
            // DC-IAM parameters (ttl)
            select block, timestamp, tx_hash,
            'DC-IAM.ilks.ttl'as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.prot_params_etl_hextobigint(right(prev_value, 12)) as from_value,
            maker.public.prot_params_etl_hextobigint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
            location like '0[%' and
            substr(location, length(location)) = '2' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    spotter_mat = pd.read_sql(f"""
            // SPOTTER parameters (mat)
            select block, timestamp, tx_hash,
            'SPOTTER.ilks.mat' as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 27) as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 27) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x65c79fcb50ca1594b025960e539ed7a9a6d434a3' and
            location like '1[%.1' and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    jug_duty = pd.read_sql(f"""
            // JUG parameters (duty)
            select block, timestamp, tx_hash,
            'JUG.ilks.duty' as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            iff(maker.public.prot_params_etl_hextobigint(prev_value)::integer > 0, round(pow(maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 27), 31536000), 4) - 1, 0)
            as from_value,
            iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer > 0, round(pow(maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 27), 31536000), 4) - 1, 0)
            as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x19c0976f590d67707e62397c87829d896dc0f1f1' and
            location like '1[%.0' and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status
    """, engine)

    dog_chop_hole = pd.read_sql(f"""
            // DOG parameters (chop, hole)
            select block, timestamp, tx_hash,
            case substr(location, length(location))
            when '1' then 'DOG.ilks.chop'
            when '2' then 'DOG.ilks.hole'
            end as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            case substr(location, length(location))
            when '1' then iff(maker.public.prot_params_etl_hextobigint(prev_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 18) - 1)
            else maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 45)
            end as from_value,
            case substr(location, length(location))
            when '1' then iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 18) - 1)
            else maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 45)
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
            location like '1[%' and
            substr(location, length(location)) in ('1', '2') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    clipper_buf_tail_cusp = pd.read_sql(f"""
            // CLIPPERs parameters (buf, tail, cusp)
            select d.block, d.timestamp, d.tx_hash,
            case substr(d.location, length(d.location))
            when '5' then 'CLIPPER.buf'
            when '6' then 'CLIPPER.tail'
            when '7' then 'CLIPPER.cusp'
            end as parameter,
            c.ilk,
            case substr(d.location, length(d.location))
            when '6' then maker.public.prot_params_etl_hextobigint(d.prev_value)::integer
            else maker.public.prot_params_etl_hextobigint(d.prev_value)::integer / pow(10, 27)
            end as from_value,
            case substr(d.location, length(d.location))
            when '6' then maker.public.prot_params_etl_hextobigint(d.curr_value)::integer
            else maker.public.prot_params_etl_hextobigint(d.curr_value)::integer / pow(10, 27)
            end as to_value
            from edw_share.raw.storage_diffs d, maker.internal.clippers c
            where d.contract = c.address and
            d.location in ('5', '6', '7') and
            from_value != to_value and
            d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
            d.status;""", engine)
    
    clipper_chip = pd.read_sql(f"""
            // CLIPPERs parameters (chip)
            select d.block, d.timestamp, d.tx_hash,
            'CLIPPER.chip' as parameter,
            c.ilk,
            maker.public.prot_params_etl_hextobigint(right(d.prev_value, 16))::integer / pow(10, 18) as from_value,
            maker.public.prot_params_etl_hextobigint(right(d.curr_value, 16))::integer / pow(10, 18) as to_value
            from edw_share.raw.storage_diffs d, maker.internal.clippers c
            where d.contract = c.address and
            d.location = '8' and
            from_value != to_value and
            d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
            d.status;""", engine)
    
    clipper_tip = pd.read_sql(f"""
            // CLIPPERs parameters (tip)
            select d.block, d.timestamp, d.tx_hash,
            'CLIPPER.tip' as parameter,
            c.ilk,
            maker.public.prot_params_etl_hextobigint(substr(d.prev_value, 1, len(d.prev_value)-16))::integer / pow(10, 45) as from_value,
            maker.public.prot_params_etl_hextobigint(substr(d.curr_value, 1, len(d.curr_value)-16))::integer / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs d, maker.internal.clippers c
            where d.contract = c.address and
            d.location = '8' and
            d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
            from_value != to_value and
            d.status;""", engine)
    
    # to big int
    vow_hump_sump_dump_bump = pd.read_sql(f"""
            // VOW parameters (hump, sump, dump, bump)
            select block, timestamp, tx_hash,
            case location
            when '8' then 'VOW.dump'
            when '9' then 'VOW.sump'
            when '10' then 'VOW.bump'
            when '11' then 'VOW.hump'
            end as parameter,
            null as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 45) as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa950524441892a31ebddf91d3ceefa04bf454466' and
            location in ('8', '9', '10', '11') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flapper_beg = pd.read_sql(f"""
            // FLAPPER parameters (beg)
            select block, timestamp, tx_hash,
            'FLAPPER.beg' as parameter,
            null as ilk,
            iff(maker.public.prot_params_etl_hextobigint(prev_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10, 18) - 1) as from_value,
            iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 18) - 1) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
            location = '4' and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flapper_ttl = pd.read_sql(f"""
            // FLAPPER parameters (ttl)
            select block, timestamp, tx_hash,
            'FLAPPER.ttl' as parameter,
            null as ilk,
            maker.public.prot_params_etl_hextobigint(concat('0x', right(prev_value, 12)))::integer as from_value,
            maker.public.prot_params_etl_hextobigint(concat('0x', right(curr_value, 12)))::integer as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
            location = '5' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flopper_bed_pad = pd.read_sql(f"""
            // FLOPPER parameters (bed, pad)
            select block, timestamp, tx_hash,
            case location
            when '4' then 'FLOPPER.beg'
            when '5' then 'FLOPPER.pad'
            end as parameter,
            null as ilk,
            iff(maker.public.prot_params_etl_hextobigint(prev_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10, 18) -1) as from_value,
            iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10, 18) -1) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
            location in ('4', '5') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flopper_ttl = pd.read_sql(f"""
            // FLOPPER parameters (ttl)
            select block, timestamp, tx_hash,
            'FLOPPER.ttl' as parameter,
            null as ilk,
            maker.public.prot_params_etl_hextobigint(concat('0x', right(prev_value, 12)))::integer as from_value,
            maker.public.prot_params_etl_hextobigint(concat('0x', right(curr_value, 12)))::integer as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
            location = '6' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    dssDirectDepositAaveDai_tau_bar = pd.read_sql(f"""
            // D3M DIRECT-AAVEV2-DAI (DssDirectDepositAaveDai) parameters (tau, bar)
            select block, timestamp, tx_hash,
            case location
            when '1' then 'D3M.tau'
            when '2' then 'D3M.bar'
            end as parameter,
            'DIRECT-AAVEV2-DAI' as ilk,
            case location
            when '2' then maker.public.prot_params_etl_hextobigint(prev_value)::integer / pow(10,27)
            else maker.public.prot_params_etl_hextobigint(prev_value)::integer
            end as from_value,
            case location
            when '2' then maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10,27)
            else maker.public.prot_params_etl_hextobigint(curr_value)::integer
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a' and
            from_value != to_value and
            location in ('1', '2') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    # too big int
    cat_chop_dunk = pd.read_sql(f"""
            // Cat Chop Dunk
            select block, timestamp, tx_hash,
            case substr(location, length(location))
            when '1' then 'CAT.ilks.chop'
            when '2' then 'CAT.ilks.dunk'
            end as parameter,
            maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
            case substr(location, length(location))
            when '1' then iff(maker.public.prot_params_etl_hextobigint(prev_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10, 18) -1)
            when '2' then (maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10, 45))
            end as from_value,
            case substr(location, length(location))
            when '1' then iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10, 18) -1)
            when '2' then (maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10, 45))
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = lower('0xa5679C04fc3d9d8b0AaB1F0ab83555b301cA70Ea') and
            location like '1[%' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flipper_tau = pd.read_sql(f"""
            select sd.block, sd.timestamp, sd.tx_hash,
            'FLIPPER.tau' as parameter,
            f.ilk,
            maker.public.prot_params_etl_hextobigint(left(sd.prev_value, 6))::integer as from_value,
            maker.public.prot_params_etl_hextobigint(left(sd.curr_value, 6))::integer as to_value
            from edw_share.raw.storage_diffs sd, maker.internal.flippers f
            where sd.contract = f.address and
            sd.status and
            sd.location = '5' and
            sd.block > {setup['start_block']} and sd.block <= {setup['end_block']} and
            from_value != to_value;""", engine)

    flipper_ttl = pd.read_sql(f"""
            select sd.block, sd.timestamp, sd.tx_hash,
            'FLIPPER.ttl' as parameter,
            f.ilk,
            iff(maker.public.prot_params_etl_hextobigint(concat('0x', right(sd.prev_value, 8))) = 0, 0, maker.public.prot_params_etl_hextobigint(concat('0x', right(sd.prev_value, 8))))::integer as from_value,
            iff(maker.public.prot_params_etl_hextobigint(concat('0x', right(sd.curr_value, 8))) = 0, 0, maker.public.prot_params_etl_hextobigint(concat('0x', right(sd.curr_value, 8))))::integer as to_value
            from edw_share.raw.storage_diffs sd, maker.internal.flippers f
            where sd.contract = f.address and
            sd.status and
            sd.location = '5' and
            sd.block > {setup['start_block']} and sd.block <= {setup['end_block']} and
            from_value != to_value;""", engine)
    
    flipper_beg = pd.read_sql(f"""
            select sd.block, sd.timestamp, sd.tx_hash,
            'FLIPPER.beg' as parameter,
            f.ilk,
            iff(maker.public.prot_params_etl_hextobigint(sd.prev_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(sd.prev_value)::integer / power(10, 18) -1) as from_value,
            iff(maker.public.prot_params_etl_hextobigint(sd.curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(sd.curr_value)::integer / power(10, 18) -1) as to_value
            from edw_share.raw.storage_diffs sd, maker.internal.flippers f
            where sd.contract = f.address and
            sd.status and
            sd.location = '4' and
            sd.block > {setup['start_block']} and sd.block <= {setup['end_block']} and
            from_value != to_value;""", engine)
    
    flopper_tau = pd.read_sql(f"""
            select block, timestamp, tx_hash, null as ilk,
            maker.public.prot_params_etl_hextobigint(concat('0x', left(substr(prev_value, 3), 6)))::integer as from_value,
            maker.public.prot_params_etl_hextobigint(concat('0x', left(substr(curr_value, 3), 6)))::integer as to_value,
            'FLOPPER.tau' as parameter
            from edw_share.raw.storage_diffs 
                where LOCATION = '6' 
                and contract = '0xa41b6ef151e06da0e34b009b86e828308986736d'
                and block > {setup['start_block']} and block <= {setup['end_block']}
                and from_value != to_value
                and status;""", engine)
    
    flapper_tau = pd.read_sql(f"""
            select block, timestamp, tx_hash, null as ilk,
            maker.public.prot_params_etl_hextobigint(concat('0x', left(substr(prev_value, 3), 6)))::integer as from_value,
            maker.public.prot_params_etl_hextobigint(concat('0x', left(substr(curr_value, 3), 6)))::integer as to_value,
            curr_value, 'FLAPPER.tau' as parameter
            from edw_share.raw.storage_diffs 
                where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' 
                and location = '5'
                and block > {setup['start_block']} and block <= {setup['end_block']}
                and from_value != to_value
                and status;""", engine)
    
    esm_min = pd.read_sql(f"""
            select block, timestamp, tx_hash, null as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10,18) as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10,18) as to_value,
            'ESM.min' as parameter
            from edw_share.raw.storage_diffs 
                where contract = '0x09e05ff6142f2f9de8b6b65855a1d56b6cfe4c58' 
                and location = '3'
                and block > {setup['start_block']} and block <= {setup['end_block']}
                and from_value != to_value
                and status;""", engine)
    
    psm_tin_tout = pd.read_sql(f"""
            select block, timestamp, tx_hash, case
            when contract = '0x961ae24a1ceba861d1fdf723794f6024dc5485cf' then 'PSM-USDP-A'
            when contract = '0x89b78cfa322f6c5de0abceecab66aee45393cc5a' then 'PSM-USDC-A'
            when contract = '0x204659b2fd2ad5723975c362ce2230fba11d3900' then 'PSM-GUSD-A'
            end as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10,18) as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10,18) as to_value,
            case
            when location = '1' then 'PSM.tin'
            when location = '2' then 'PSM.tout'
            end as parameter
            from edw_share.raw.storage_diffs 
                where contract in ('0x961ae24a1ceba861d1fdf723794f6024dc5485cf', '0x89b78cfa322f6c5de0abceecab66aee45393cc5a', '0x204659b2fd2ad5723975c362ce2230fba11d3900')
                and location in ('1', '2')
                and from_value != to_value
                and block > {setup['start_block']} and block <= {setup['end_block']}
                and status;""", engine)
    
    dspause_delay = pd.read_sql(f"""
            select block, timestamp, tx_hash, null as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer as from_value,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer as to_value,
            'DSPAUSE.delay' as parameter
            from edw_share.raw.storage_diffs 
                where contract = '0xbe286431454714f511008713973d3b053a2d38f3' 
                and location = '4'
                and from_value != to_value
                and block > {setup['start_block']} and block <= {setup['end_block']}
                and status;
            """, engine)
    
    end_wait = pd.read_sql(f"""
            select block, timestamp, tx_hash, null as ilk,
            maker.public.prot_params_etl_hextobigint(prev_value)::integer,
            maker.public.prot_params_etl_hextobigint(curr_value)::integer,
            'END.wait' as parameter
            from edw_share.raw.storage_diffs 
                where contract = '0xbb856d1742fd182a90239d7ae85706c2fe4e5922' 
                and location = '9';""", engine)

    # Concatenate results into one df and return
    protocol_params: pd.DataFrame = pd.concat([
        vat_line_dust,
        dciam_line_gap,
        dciam_ttl,
        spotter_mat,
        jug_duty,
        dog_chop_hole,
        clipper_buf_tail_cusp,
        clipper_chip,
        clipper_tip,
        vow_hump_sump_dump_bump,
        flapper_beg,
        flapper_ttl,
        flopper_bed_pad,
        flopper_ttl,
        dssDirectDepositAaveDai_tau_bar,
        cat_chop_dunk,
        flipper_tau,
        flipper_ttl,
        flipper_beg,
        flopper_tau,
        flapper_tau,
        esm_min,
        psm_tin_tout,
        dspause_delay,
        end_wait
    ]).reset_index(drop=True)

    return protocol_params


def apply_source_types(protocol_params: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Identify and apply source column for protocol parameters
    """

    # Fetch contextual data
    lerps = fetch_lerps(engine)

    # Iterate through rows and populate source column
    for idx in range(len(protocol_params)):
        if 'IAM' in protocol_params.loc[idx, 'PARAMETER']:
            protocol_params.loc[idx, 'SOURCE_TYPE'] = 'DC-IAM'
        elif protocol_params.loc[idx, 'SOURCE'] in lerps.TO_ADDRESS.values:
            protocol_params.loc[idx, 'SOURCE_TYPE'] = 'lerp'
        else:
            protocol_params.loc[idx, 'SOURCE_TYPE'] = 'dsspell'

    return protocol_params


def fetch_lerps(engine) -> pd.DataFrame:
    """
    Fetch list of lerps for source identification
    """

    # Read lerps from edw
    lerps = pd.read_sql("""
        // Fetching all LERP addresses
        SELECT to_address
        FROM edw_share.raw.calls
        WHERE from_address = '0x9175561733d138326fdea86cdfdf53e92b588276'
        AND tx_hash in (SELECT tx_hash FROM edw_share.raw.state_diffs WHERE reason = 'contract creation');
    """, engine)

    return lerps


def apply_sources(protocol_params: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Function to fetch contract sources
    """

    print(protocol_params)

    # Fetch contract sources
    sources = pd.read_sql(
        f"""SELECT TX_HASH, TO_ADDRESS
            FROM edw_share.raw.transactions
            WHERE TX_HASH IN ({','.join(list(map(lambda x: f"'{x}'", protocol_params.TX_HASH.unique())))})""",
        engine
    )

    # Apply contract sources and create source type column
    protocol_params['SOURCE'] = protocol_params.TX_HASH.apply(
        lambda x: sources[sources.TX_HASH == x].values[0][1]
    )
    protocol_params['SOURCE_TYPE'] = None

    return protocol_params


def _load(engine, **setup):

    # Fetch Clippers & Flippers
    load_clips_flips(**setup)

    # Fetch result dataframe
    protocol_params = fetch_params(engine, setup)

    # Apply sources
    protocol_params = apply_sources(protocol_params, engine)

    # Apply source types
    protocol_params = apply_source_types(protocol_params, engine)

    # Write to table
    protocol_params.to_sql(f"""{setup['target_db'].split('.')[2]}""", sa, schema=f"""{setup['target_db'].split('.')[0] + '.' + setup['target_db'].split('.')[1]}""", index=False, if_exists='append')

    sf.execute(
        f"""
            insert into {setup['scheduler']} (load_id, start_block, end_block)
            values('{setup['load_id']}', {setup['start_block']}, {setup['end_block']});
        """
    )

    return