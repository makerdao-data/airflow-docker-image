import pandas as pd

import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, sa
from dags.utils.parameters.load_clippers import load_clips
from dags.utils.parameters.load_flippers import load_flips
from dags.utils.parameters.load_floppers import load_flops
from dags.utils.parameters.load_flappers import load_flaps
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage


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
                if (S.substring(0,2) !== "0x") {
                    _int = BigInt("0x" + S, 16);
                }
                else {
                    _int = BigInt(S, 16);
                }
                
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
        maker.public.prot_params_etl_hextobigint(prev_value) / pow(10, 45)::float as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value) / pow(10, 45)::float as to_value
        from edw_share.raw.storage_diffs
        where contract = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b' and
        location like '2[%' and
        substr(location, length(location)) in ('3' ,'4') and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        from_value != to_value and
        status;""", engine)

    dciam_line_gap = pd.read_sql(f"""
        // DC-IAM parameters (line, gap)
        select block, timestamp, tx_hash,
        case substr(location, length(location))
        when '0' then 'DC-IAM.ilks.line'
        when '1' then 'DC-IAM.ilks.gap'
        end as parameter,
        maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
        maker.public.prot_params_etl_hextobigint(prev_value) / pow(10, 45)::float  as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value) / pow(10, 45)::float  as to_value
        from edw_share.raw.storage_diffs
        where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
        location like '0[%' and
        substr(location, length(location)) in ('0', '1') and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        from_value != to_value and
        status;""", engine)

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
        status;""", engine)

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
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

    jug_duty = pd.read_sql(f"""
        // JUG parameters (duty)
        select block, timestamp, tx_hash,
        'JUG.ilks.duty' as parameter,
        maker.public.prot_params_etl_hextostr(substr(location, 3, 42)) as ilk,
        iff(maker.public.prot_params_etl_hextobigint(prev_value)::integer > 0, round(pow(maker.public.prot_params_etl_hextobigint(prev_value) / pow(10, 27), 31536000), 4) - 1, 0)
        as from_value,
        iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer > 0, round(pow(maker.public.prot_params_etl_hextobigint(curr_value) / pow(10, 27), 31536000), 4) - 1, 0)
        as to_value
        from edw_share.raw.storage_diffs
        where contract = '0x19c0976f590d67707e62397c87829d896dc0f1f1' and
        location like '1[%.0' and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

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
        else maker.public.prot_params_etl_hextobigint(prev_value) / pow(10, 45)::float 
        end as from_value,
        case substr(location, length(location))
        when '1' then iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(curr_value)::integer / pow(10, 18) - 1)
        else maker.public.prot_params_etl_hextobigint(curr_value) / pow(10, 45)::float 
        end as to_value
        from edw_share.raw.storage_diffs
        where contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
        location like '1[%' and
        substr(location, length(location)) in ('1', '2') and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

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
        when '6' then maker.public.prot_params_etl_hextobigint(d.prev_value)
        else maker.public.prot_params_etl_hextobigint(d.prev_value) / pow(10, 27)
        end as from_value,
        case substr(d.location, length(d.location))
        when '6' then maker.public.prot_params_etl_hextobigint(d.curr_value)
        else maker.public.prot_params_etl_hextobigint(d.curr_value) / pow(10, 27)
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
        maker.public.prot_params_etl_hextobigint(right(d.curr_value, 16))::integer  / pow(10, 18) as to_value
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
        maker.public.prot_params_etl_hextobigint(substr(d.prev_value, 1, len(d.prev_value)-16)) / pow(10, 45)::float  as from_value,
        maker.public.prot_params_etl_hextobigint(substr(d.curr_value, 1, len(d.curr_value)-16)) / pow(10, 45)::float  as to_value
        from edw_share.raw.storage_diffs d, maker.internal.clippers c
        where d.contract = c.address and
        d.location = '8' and
        d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
        from_value != to_value and
        d.status;""", engine)

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
        maker.public.prot_params_etl_hextobigint(prev_value) / pow(10, 45)::float  as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value) / pow(10, 45)::float  as to_value
        from edw_share.raw.storage_diffs
        where contract = '0xa950524441892a31ebddf91d3ceefa04bf454466' and
        location in ('8', '9', '10', '11') and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

    vow_wait = pd.read_sql(f"""
        // VOW.wait
        select block, timestamp, tx_hash,
        case location
        when '7' then 'VOW.wait'
        end as parameter,
        null as ilk,
        maker.public.prot_params_etl_hextobigint(prev_value) as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value) as to_value
        from edw_share.raw.storage_diffs
        where contract = '0xa950524441892a31ebddf91d3ceefa04bf454466' and
        location in ('7') and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

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
        maker.public.prot_params_etl_hextobigint(right(prev_value, 12))::integer as from_value,
        maker.public.prot_params_etl_hextobigint(right(curr_value, 12))::integer as to_value
        from edw_share.raw.storage_diffs
        where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
        location = '5' and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

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
        status;""", engine)

    flopper_ttl = pd.read_sql(f"""
        // FLOPPER parameters (ttl)
        select block, timestamp, tx_hash,
        'FLOPPER.ttl' as parameter,
        null as ilk,
        maker.public.prot_params_etl_hextobigint(right(prev_value, 12))::integer as from_value,
        maker.public.prot_params_etl_hextobigint(right(curr_value, 12))::integer as to_value
        from edw_share.raw.storage_diffs
        where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
        location = '6' and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

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
        status;""", engine)

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
        when '2' then (maker.public.prot_params_etl_hextobigint(prev_value) / power(10, 45))
        end as from_value,
        case substr(location, length(location))
        when '1' then iff(maker.public.prot_params_etl_hextobigint(curr_value)::integer = 0, 0, maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10, 18) -1)
        when '2' then (maker.public.prot_params_etl_hextobigint(curr_value) / power(10, 45))
        end as to_value
        from edw_share.raw.storage_diffs
        where contract = lower('0xa5679C04fc3d9d8b0AaB1F0ab83555b301cA70Ea') and
        location like '1[%' and
        from_value != to_value and
        block > {setup['start_block']} and block <= {setup['end_block']} and
        status;""", engine)

    flipper_tau = pd.read_sql(f"""
        select sd.block, sd.timestamp, sd.tx_hash,
        'FLIPPER.tau' as parameter,
        f.ilk,
        maker.public.prot_params_etl_hextobigint(substr(sd.prev_value, 1, len(sd.prev_value)-12)) as from_value,
        maker.public.prot_params_etl_hextobigint(substr(sd.curr_value, 1, len(sd.curr_value)-12)) as to_value
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
        maker.public.prot_params_etl_hextobigint(right(prev_value, 12)) as from_value,
        maker.public.prot_params_etl_hextobigint(right(curr_value, 12)) as to_value
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
        iff(maker.public.prot_params_etl_hextobigint(sd.prev_value) / power(10, 18) = 0, 0, maker.public.prot_params_etl_hextobigint(sd.prev_value) / power(10, 18) -1) as from_value,
        iff(maker.public.prot_params_etl_hextobigint(sd.curr_value) / power(10, 18) = 0, 0, maker.public.prot_params_etl_hextobigint(sd.curr_value) / power(10, 18) -1) as to_value
        from edw_share.raw.storage_diffs sd, maker.internal.flippers f
        where sd.contract = f.address and
        sd.status and
        sd.location = '4' and
        sd.block > {setup['start_block']} and sd.block <= {setup['end_block']} and
        from_value != to_value;""", engine)
    
    flopper_tau = pd.read_sql(f"""
        select block, timestamp, tx_hash, 'FLOPPER.tau' as parameter, null as ilk,
        maker.public.prot_params_etl_hextobigint(substr(prev_value, 1, len(prev_value)-12)) as from_value,
        maker.public.prot_params_etl_hextobigint(substr(curr_value, 1, len(curr_value)-12)) as to_value
        from edw_share.raw.storage_diffs 
            where LOCATION = '6' 
            and contract = '0xa41b6ef151e06da0e34b009b86e828308986736d'
            and block > {setup['start_block']} and block <= {setup['end_block']}
            and from_value != to_value
            and status;""", engine)
    
    flapper_tau = pd.read_sql(f"""
        select block, timestamp, tx_hash, 'FLAPPER.tau' as parameter, null as ilk,
        maker.public.prot_params_etl_hextobigint(substr(prev_value, 1, len(prev_value)-12)) as from_value,
        maker.public.prot_params_etl_hextobigint(substr(curr_value, 1, len(curr_value)-12)) as to_value
        from edw_share.raw.storage_diffs 
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' 
            and location = '5'
            and block > {setup['start_block']} and block <= {setup['end_block']}
            and from_value != to_value
            and status;""", engine)
    
    esm_min = pd.read_sql(f"""
        select block, timestamp, tx_hash, 'ESM.min' as parameter, null as ilk,
        maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10,18) as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10,18) as to_value
        from edw_share.raw.storage_diffs 
            where contract = '0x09e05ff6142f2f9de8b6b65855a1d56b6cfe4c58' 
            and location = '3'
            and block > {setup['start_block']} and block <= {setup['end_block']}
            and from_value != to_value
            and status;""", engine)
    
    psm_tin_tout = pd.read_sql(f"""
        select block, timestamp, tx_hash, case
        when location = '1' then 'PSM.tin'
        when location = '2' then 'PSM.tout'
        end as parameter,
        case
        when contract = '0x961ae24a1ceba861d1fdf723794f6024dc5485cf' then 'PSM-USDP-A'
        when contract = '0x89b78cfa322f6c5de0abceecab66aee45393cc5a' then 'PSM-USDC-A'
        when contract = '0x204659b2fd2ad5723975c362ce2230fba11d3900' then 'PSM-GUSD-A'
        end as ilk,
        maker.public.prot_params_etl_hextobigint(prev_value)::integer / power(10,18) as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value)::integer / power(10,18) as to_value
        from edw_share.raw.storage_diffs 
            where contract in ('0x961ae24a1ceba861d1fdf723794f6024dc5485cf', '0x89b78cfa322f6c5de0abceecab66aee45393cc5a', '0x204659b2fd2ad5723975c362ce2230fba11d3900')
            and location in ('1', '2')
            and from_value != to_value
            and block > {setup['start_block']} and block <= {setup['end_block']}
            and status;""", engine)
    
    dspause_delay = pd.read_sql(f"""
        select block, timestamp, tx_hash, 'DSPAUSE.delay' as parameter, null as ilk,
        maker.public.prot_params_etl_hextobigint(prev_value)::integer as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value)::integer as to_value
        from edw_share.raw.storage_diffs 
            where contract = '0xbe286431454714f511008713973d3b053a2d38f3' 
            and location = '4'
            and from_value != to_value
            and block > {setup['start_block']} and block <= {setup['end_block']}
            and status;
        """, engine)
    
    end_wait = pd.read_sql(f"""
        select block, timestamp, tx_hash, 'END.wait' as parameter, null as ilk,
        maker.public.prot_params_etl_hextobigint(prev_value)::integer as from_value,
        maker.public.prot_params_etl_hextobigint(curr_value)::integer as to_value
        from edw_share.raw.storage_diffs 
            where contract = '0xbb856d1742fd182a90239d7ae85706c2fe4e5922' 
            and location = '9'
            and status
            and from_value != to_value
            and block > {setup['start_block']} and block <= {setup['end_block']};""", engine)

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
        vow_wait,
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
    lerps = pd.read_sql(f"""
        SELECT LERP
        FROM MAKER.INTERNAL.LERPS;
    """, engine)

    # Iterate through rows and populate source column
    for idx in range(len(protocol_params)):
        if protocol_params.loc[idx, 'SOURCE'] == '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3':
            protocol_params.loc[idx, 'SOURCE_TYPE'] = 'DssAutoLine'
        elif protocol_params.loc[idx, 'SOURCE'] in lerps.LERP.values:
            protocol_params.loc[idx, 'SOURCE_TYPE'] = 'Lerp'
        else:
            protocol_params.loc[idx, 'SOURCE_TYPE'] = 'DssSpell'

    return protocol_params


def load_lerps(sf, **setup) -> pd.DataFrame:
    """
    Fetch list of lerps for source identification
    """

    # Read lerps from edw
    lerps = sf.execute(f"""
        // Fetching all LERP addresses
        SELECT block, timestamp, concat('0x', lpad(ltrim(tx_hash, '0x'), 64, '0')) as tx_hash,
        concat('0x', lpad(ltrim(to_address, '0x'), 40, '0')) as lerp
        FROM edw_share.raw.calls
        WHERE from_address = '0x9175561733d138326fdea86cdfdf53e92b588276'
        AND block > {setup['start_block']}
        AND block <= {setup['end_block']}
        AND tx_hash in (SELECT tx_hash
                        FROM edw_share.raw.state_diffs
                        WHERE reason = 'contract creation'
                        AND block > {setup['start_block']}
                        AND block <= {setup['end_block']});
    """).fetchall()

    if lerps:
        pattern = _write_to_stage(sf, lerps, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"MAKER.INTERNAL.LERPS",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)

    return


def apply_sources(protocol_params: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Function to fetch contract sources
    """

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

    # Fetch Clippers & Flippers & Flappers & Floppers
    load_clips(**setup)
    load_flips(**setup)
    load_flaps(**setup)
    load_flops(**setup)

    load_lerps(sf, **setup)

    # Fetch result dataframe
    protocol_params = fetch_params(engine, setup)

    # Apply sources
    protocol_params = apply_sources(protocol_params, engine)

    # Apply source types
    protocol_params = apply_source_types(protocol_params, engine)

    # Write to table
    # protocol_params.to_sql(f"""{setup['target_db'].split('.')[2]}""", sa, schema=f"""{setup['target_db'].split('.')[0] + '.' + setup['target_db'].split('.')[1]}""", index=False, if_exists='append')

    if not protocol_params.empty:
        pp = list()
        # {
        # 'BLOCK': 13551743, 
        # 'TIMESTAMP': datetime.datetime(2021, 11, 4, 18, 10, 38), 
        # 'TX_HASH': '0x5f4de74b2f02b5241141d7510f3d606983937fd68e19f94b1c7cd6f179a185de', 
        # 'PARAMETER': 'VAT.ilks.line', 
        # 'ILK': 'ETH-A', 
        # 'FROM_VALUE': 2873530165.138446, 
        # 'TO_VALUE': 2875565887.508294, 
        # 'SOURCE': '0x315ba6fbd305fcc41d0febe6698c4144c903c24a', 
        # 'SOURCE_TYPE': 'DssSpell'
        # }

        # for block, timestamp, tx_hash, parameter, ilk, from_value, to_value, source, source_type in protocol_params.values.tolist():
        for d in protocol_params.to_dict('records'):
            pp.append([
                int(d['BLOCK']),
                d['TIMESTAMP'].__str__()[:19],
                d['TX_HASH'],
                d['SOURCE'],
                d['PARAMETER'],
                d['ILK'],
                float(d['FROM_VALUE']),
                float(d['TO_VALUE']),
                d['SOURCE_TYPE']
            ])
        pattern = _write_to_stage(sf, pp, f"MAKER.PUBLIC.PARAMETERS_STORAGE")
        if pattern:
            _write_to_table(
                sf,
                f"MAKER.PUBLIC.PARAMETERS_STORAGE",
                f"{setup['target_db'].split('.')[0]}.{setup['target_db'].split('.')[1]}.{setup['target_db'].split('.')[2]}",
                pattern,
            )
            _clear_stage(sf, f"MAKER.PUBLIC.PARAMETERS_STORAGE", pattern)

    sf.execute(
        f"""
            insert into {setup['scheduler']} (load_id, start_block, end_block)
            values('{setup['load_id']}', {setup['start_block']}, {setup['end_block']});
        """
    )

    return