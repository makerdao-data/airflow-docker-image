from dags.connectors.sf import sf, connection
import pandas as pd


def fetch_params(engine, setup) -> pd.DataFrame:
    """
    Function to fetch protocol parameters
    """

    # Create type conversion function in-snowflake
    sf.execute(
        """
            create or replace function maker.public.etl_hextostr (s string)
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
            create or replace function maker.public.etl_hextoint (s string)
            returns double language JAVASCRIPT
            as
            'if (S !== null && S !== "" && S !== "0x") {
            _int = parseInt(S, 16);
            }
            else {
            _int = 0;
            }
            return _int';
        """
    )

    # sf.execute("""
    #     insert into maker.public.parameters (
    #     with
    #     clippers as
    #     (select distinct maker.public.etl_hextostr(substr(sd.location, 3, 42)) as ilk,
    #     sd.curr_value as address,
    #     sd.tx_hash,
    #     txs.to_address as DssSpell
    #     from edw_share.raw.storage_diffs sd, edw_share.raw.transactions txs
    #     where sd.tx_hash = txs.tx_hash and
    #     sd.contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
    #     sd.location like '1[%' and
    #     substr(sd.location, length(sd.location)) = '0' and
    #     sd.status),
    #     flippers as
    #     (select distinct maker.public.etl_hextostr(substr(location, 3, 42)) as ilk, curr_value as address
    #     from edw_share.raw.storage_diffs
    #     where contract = lower('0xa5679C04fc3d9d8b0AaB1F0ab83555b301cA70Ea') and
    #     location like '1[%' and
    #     substr(location, length(location)) = '0'and
    #     status)
    #     select p.block, p.timestamp, p.tx_hash,
    #     case
    #     when t.to_address is null then p.DssSpell
    #     else t.to_address
    #     end as source,
    #     p.parameter, p.ilk, p.from_value, p.to_value from
    # (

    vat_line_dust = pd.read_sql(f"""
            // VAT parameters (line, dust)
            select block, timestamp, tx_hash, order_index,
            iff(substr(location, length(location)) = '3', 'VAT.ilks.line', 'VAT.ilks.dust') as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.etl_hextoint(prev_value) / pow(10, 45) as from_value,
            maker.public.etl_hextoint(curr_value) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b' and
            location like '2[%' and
            substr(location, length(location)) in ('3' ,'4') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    dciam_line_gap = pd.read_sql(f"""
            // DC-IAM parameters (line, gap)
            select block, timestamp, tx_hash, order_index,
            case substr(location, length(location))
            when '0' then 'DC-IAM.ilks.line'
            when '1' then 'DC-IAM.ilks.gap'
            end as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.etl_hextoint(prev_value) / pow(10, 45) as from_value,
            maker.public.etl_hextoint(curr_value) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
            location like '0[%' and
            substr(location, length(location)) in ('0', '1') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    dciam_ttl = pd.read_sql(f"""
            // DC-IAM parameters (ttl)
            select block, timestamp, tx_hash, order_index,
            'DC-IAM.ilks.ttl'as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.etl_hextoint(right(prev_value, 12)) as from_value,
            maker.public.etl_hextoint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
            location like '0[%' and
            substr(location, length(location)) = '2' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    spotter_mat = pd.read_sql(f"""
            // SPOTTER parameters (mat)
            select block, timestamp, tx_hash, order_index,
            'SPOTTER.ilks.mat' as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            maker.public.etl_hextoint(prev_value) / pow(10, 27) as from_value,
            maker.public.etl_hextoint(curr_value) / pow(10, 27) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x65c79fcb50ca1594b025960e539ed7a9a6d434a3' and
            location like '1[%.1' and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    jug_duty = pd.read_sql(f"""
            // JUG parameters (duty)
            select block, timestamp, tx_hash, order_index,
            'JUG.ilks.duty' as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            iff(maker.public.etl_hextoint(prev_value) > 0, round(pow(maker.public.etl_hextoint(prev_value) / pow(10, 27), 31536000), 4) - 1, 0)
            as from_value,
            iff(maker.public.etl_hextoint(curr_value) > 0, round(pow(maker.public.etl_hextoint(curr_value) / pow(10, 27), 31536000), 4) - 1, 0)
            as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x19c0976f590d67707e62397c87829d896dc0f1f1' and
            location like '1[%.0' and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status
    """, engine)

    dog_chop_hole = pd.read_sql(f"""
            // DOG parameters (chop, hole)
            select block, timestamp, tx_hash, order_index,
            case substr(location, length(location))
            when '1' then 'DOG.ilks.chop'
            when '2' then 'DOG.ilks.hole'
            end as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            case substr(location, length(location))
            when '1' then iff(maker.public.etl_hextoint(prev_value) = 0, 0, maker.public.etl_hextoint(prev_value) / pow(10, 18) - 1)
            else maker.public.etl_hextoint(prev_value) / pow(10, 45)
            end as from_value,
            case substr(location, length(location))
            when '1' then iff(maker.public.etl_hextoint(curr_value) = 0, 0, maker.public.etl_hextoint(curr_value) / pow(10, 18) - 1)
            else maker.public.etl_hextoint(curr_value) / pow(10, 45)
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
            location like '1[%' and
            substr(location, length(location)) in ('1', '2') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    # clipper_buf_tail_cusp = pd.read_sql(f"""
    #         // CLIPPERs parameters (buf, tail, cusp)
    #         select d.block, d.timestamp, d.tx_hash, d.order_index,
    #         case substr(d.location, length(d.location))
    #         when '5' then 'CLIPPER.buf'
    #         when '6' then 'CLIPPER.tail'
    #         when '7' then 'CLIPPER.cusp'
    #         end as parameter,
    #         c.ilk,
    #         case substr(d.location, length(d.location))
    #         when '6' then maker.public.etl_hextoint(d.prev_value)
    #         else maker.public.etl_hextoint(d.prev_value) / pow(10, 27)
    #         end as from_value,
    #         case substr(d.location, length(d.location))
    #         when '6' then maker.public.etl_hextoint(d.curr_value)
    #         else maker.public.etl_hextoint(d.curr_value) / pow(10, 27)
    #         end as to_value,
    #         c.DssSpell
    #         from edw_share.raw.storage_diffs d, clippers c
    #         where d.contract = c.address and
    #         d.location in ('5', '6', '7') and
    #         d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
    #         d.status""", engine)
    #
    # clipper_chip = pd.read_sql(f"""
    #         // CLIPPERs parameters (chip)
    #         select d.block, d.timestamp, d.tx_hash, d.order_index,
    #         'CLIPPER.chip' as parameter,
    #         c.ilk,
    #         maker.public.etl_hextoint(right(d.prev_value, 16)) / pow(10, 18) as from_value,
    #         maker.public.etl_hextoint(right(d.curr_value, 16)) / pow(10, 18) as to_value,
    #         'DssSpell' as DssSpell
    #         from edw_share.raw.storage_diffs d, clippers c
    #         where d.contract = c.address and
    #         d.location = '8' and
    #         from_value != to_value and
    #         d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
    #         d.status""", engine)
    #
    # clipper_tip = pd.read_sql(f"""
    #         // CLIPPERs parameters (tip)
    #         select d.block, d.timestamp, d.tx_hash, d.order_index,
    #         'CLIPPER.tip' as parameter,
    #         c.ilk,
    #         maker.public.etl_hextoint(substr(d.prev_value, 1, len(d.prev_value)-16)) / pow(10, 45) as from_value,
    #         maker.public.etl_hextoint(substr(d.curr_value, 1, len(d.curr_value)-16)) / pow(10, 45) as to_value,
    #         'DssSpell' as DssSpell
    #         from edw_share.raw.storage_diffs d, clippers c
    #         where d.contract = c.address and
    #         d.location = '8' and
    #         from_value != to_value and
    #         d.block > {setup['start_block']} and d.block <= {setup['end_block']} and
    #         d.status""", engine)

    vow_hump_sump_dump_bump = pd.read_sql(f"""
            // VOW parameters (hump, sump, dump, bump)
            select block, timestamp, tx_hash, order_index,
            case location
            when '8' then 'VOW.dump'
            when '9' then 'VOW.sump'
            when '10' then 'VOW.bump'
            when '11' then 'VOW.hump'
            end as parameter,
            null as ilk,
            maker.public.etl_hextoint(prev_value) / pow(10, 45) as from_value,
            maker.public.etl_hextoint(curr_value) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa950524441892a31ebddf91d3ceefa04bf454466' and
            location in ('8', '9', '10', '11') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flapper_beg = pd.read_sql(f"""
            // FLAPPER parameters (beg)
            select block, timestamp, tx_hash, order_index,
            'FLAPPER.beg' as parameter,
            null as ilk,
            iff(maker.public.etl_hextoint(prev_value) = 0, 0, maker.public.etl_hextoint(prev_value) / pow(10, 18) - 1) as from_value,
            iff(maker.public.etl_hextoint(curr_value) = 0, 0, maker.public.etl_hextoint(curr_value) / pow(10, 18) - 1) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
            location = '4' and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flapper_ttl = pd.read_sql(f"""
            // FLAPPER parameters (ttl)
            select block, timestamp, tx_hash, order_index,
            'FLAPPER.ttl' as parameter,
            null as ilk,
            maker.public.etl_hextoint(right(prev_value, 12)) as from_value,
            maker.public.etl_hextoint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
            location = '5' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flopper_bed_pad = pd.read_sql(f"""
            // FLOPPER parameters (bed, pad)
            select block, timestamp, tx_hash, order_index,
            case location
            when '4' then 'FLOPPER.beg'
            when '5' then 'FLOPPER.pad'
            end as parameter,
            null as ilk,
            iff(maker.public.etl_hextoint(prev_value) = 0, 0, maker.public.etl_hextoint(prev_value) / power(10, 18) -1) as from_value,
            iff(maker.public.etl_hextoint(curr_value) = 0, 0, maker.public.etl_hextoint(curr_value) / power(10, 18) -1) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
            location in ('4', '5') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    flopper_ttl = pd.read_sql(f"""
            // FLOPPER parameters (ttl)
            select block, timestamp, tx_hash, order_index,
            'FLOPPER.ttl' as parameter,
            null as ilk,
            maker.public.etl_hextoint(right(prev_value, 12)) as from_value,
            maker.public.etl_hextoint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
            location = '6' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    dssDirectDepositAaveDai_tau_bar = pd.read_sql(f"""
            // D3M DIRECT-AAVEV2-DAI (DssDirectDepositAaveDai) parameters (tau, bar)
            select block, timestamp, tx_hash, order_index,
            case location
            when '1' then 'D3M.tau'
            when '2' then 'D3M.bar'
            end as parameter,
            'DIRECT-AAVEV2-DAI' as ilk,
            case location
            when '2' then maker.public.etl_hextoint(prev_value) / pow(10,27)
            else maker.public.etl_hextoint(prev_value)
            end as from_value,
            case location
            when '2' then maker.public.etl_hextoint(curr_value) / pow(10,27)
            else maker.public.etl_hextoint(curr_value)
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a' and
            from_value != to_value and
            location in ('1', '2') and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    cat_chop_dunk = pd.read_sql(f"""
            // Cat Chop Dunk
            select block, timestamp, tx_hash, order_index,
            case substr(location, length(location))
            when '1' then 'CAT.ilks.chop'
            when '2' then 'CAT.ilks.dunk'
            end as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            case substr(location, length(location))
            when '1' then iff(maker.public.etl_hextoint(prev_value) = 0, 0, maker.public.etl_hextoint(prev_value) / power(10, 18) -1)
            when '2' then (maker.public.etl_hextoint(prev_value) / power(10, 45))
            end as from_value,
            case substr(location, length(location))
            when '1' then iff(maker.public.etl_hextoint(curr_value) = 0, 0, maker.public.etl_hextoint(curr_value) / power(10, 18) -1)
            when '2' then (maker.public.etl_hextoint(curr_value) / power(10, 45))
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = lower('0xa5679C04fc3d9d8b0AaB1F0ab83555b301cA70Ea') and
            location like '1[%' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and
            status""", engine)

    #      select sd.block, sd.timestamp, sd.tx_hash, sd.order_index,
    #     'FLIPPER.tau' as parameter,
    #     f.ilk,
    #     maker.public.etl_hextoint(substr(sd.prev_value, 0, 8)) as from_value,
    #     maker.public.etl_hextoint(substr(sd.curr_value, 0, 8)) as to_value,
    #     'DssSpell' as DssSpell
    #     from edw_share.raw.storage_diffs sd, flippers f
    #     where sd.contract = f.address and
    #     sd.status and
    #     sd.location = '5' and
    #     from_value != to_value and
    #     sd.block > {setup['start_block']} and sd.block <= {setup['end_block']}
    #     union
    #     select sd.block, sd.timestamp, sd.tx_hash, sd.order_index,
    #     'FLIPPER.ttl' as parameter,
    #     f.ilk,
    #     maker.public.etl_hextoint(substr(sd.prev_value, 8)) as from_value,
    #     maker.public.etl_hextoint(concat('0x', substr(sd.curr_value, 8))) as to_value,
    #     'DssSpell' as DssSpell
    #     from edw_share.raw.storage_diffs sd, flippers f
    #     where sd.contract = f.address and
    #     sd.status and
    #     sd.location = '5' and
    #     from_value != to_value and
    #     sd.block > {setup['start_block']} and sd.block <= {setup['end_block']}
    #     union
    #     select sd.block, sd.timestamp, sd.tx_hash, sd.order_index,
    #     'FLIPPER.beg' as parameter,
    #     f.ilk,
    #     iff(maker.public.etl_hextoint(sd.prev_value) = 0, 0, maker.public.etl_hextoint(sd.prev_value) / power(10, 18) -1) as from_value,
    #     iff(maker.public.etl_hextoint(sd.curr_value) = 0, 0, maker.public.etl_hextoint(sd.curr_value) / power(10, 18) -1) as to_value,
    #     'DssSpell' as DssSpell
    #     from edw_share.raw.storage_diffs sd, flippers f
    #     where sd.contract = f.address and
    #     sd.status and
    #     sd.location = '4' and
    #     from_value != to_value and
    #     sd.block > {setup['start_block']} and sd.block <= {setup['end_block']}
    #
    #     ) p join edw_share.raw.transactions t on p.tx_hash = t.tx_hash
    #     order by block desc, order_index desc);
    # """

    # Concatenate results into one df and return
    protocol_params: pd.DataFrame = pd.concat([
        vat_line_dust,
        dciam_line_gap,
        dciam_ttl,
        spotter_mat,
        jug_duty,
        dog_chop_hole,
        # clipper_buf_tail_cusp,
        # clipper_chip,
        # clipper_tip,
        vow_hump_sump_dump_bump,
        flapper_beg,
        flapper_ttl,
        flopper_bed_pad,
        flopper_ttl,
        dssDirectDepositAaveDai_tau_bar,
        cat_chop_dunk
    ]).reset_index(drop=True).drop(columns='order_index')

    return protocol_params


def apply_source_types(protocol_params: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Identify and apply source column for protocol parameters
    """

    # Fetch contextual data
    lerps = fetch_lerps(engine)

    # Iterate through rows and populate source column
    for idx in range(len(protocol_params)):
        if 'IAM' in protocol_params.loc[idx, 'parameter']:
            protocol_params.loc[idx, 'source_type'] = 'DC-IAM'
        elif protocol_params.loc[idx, 'source'] in lerps.to_address.values:
            protocol_params.loc[idx, 'source_type'] = 'lerp'
        else:
            protocol_params.loc[idx, 'source_type'] = 'dsspell'

    return protocol_params


def fetch_lerps(engine) -> pd.DataFrame:
    """
    Fetch list of lerps for source identification
    """

    # Read lerps from edw
    lerps = pd.read_sql("""
        // Fetching all LERP addresses
        SELECT   to_address
        FROM     edw_share.raw.calls
        WHERE    from_address = '0x9175561733d138326fdea86cdfdf53e92b588276'
        AND tx_hash in (SELECT tx_hash
        FROM   edw_share.raw.state_diffs
        WHERE    reason = 'contract creation')
    """, engine)

    return lerps


def apply_sources(protocol_params: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Function to fetch contract sources
    """

    # Fetch contract sources
    sources = pd.read_sql(
        f"""select tx_hash, to_address from edw_share.raw.transactions where tx_hash in ({','.join(list(map(lambda x: f"'{x}'", protocol_params.tx_hash.unique())))})""",
        engine
    )

    # Apply contract sources and create source type column
    protocol_params['source'] = protocol_params.tx_hash.apply(
        lambda x: sources[sources.tx_hash == x].values[0][1]
    )
    protocol_params['source_type'] = None

    return protocol_params


def _load(engine, setup):

    # Fetch result dataframe
    protocol_params = fetch_params(engine, setup)

    # Apply sources
    protocol_params = apply_sources(protocol_params, engine)

    # Apply source types
    protocol_params = apply_source_types(protocol_params, engine)

    # Write to table
    protocol_params.to_sql('test_parameters', conn, schema='maker.public', index=False, if_exists='append')

    return