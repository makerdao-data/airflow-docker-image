from dags.connectors.sf import sf


def _load(**setup):

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

    sf.execute(
        f"""

            insert into customers.dicu.parameters (
            with clippers as
            (select distinct maker.public.etl_hextostr(substr(location, 3, 42)) as ilk, curr_value as address
            from edw_share.raw.storage_diffs
            where contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
            location like '1[%' and
            substr(location, length(location)) = '0' and
            block > {setup['start_block']} and block <= {setup['end_block']} and status)

            select p.block, p.timestamp, p.tx_hash, t.to_address as source,
            p.parameter, p.ilk, p.from_value, p.to_value from
            (

            // VAT parameters (line, dust)
            select block, timestamp, tx_hash, order_index,
            iff(substr(location, length(location)) = '3', 'VAT.ilks.line', 'VAT.ilks.dust') as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            etl_hextoint(prev_value) / pow(10, 45) as from_value,
            etl_hextoint(curr_value) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b' and
            location like '2[%' and
            substr(location, length(location)) in ('3' ,'4') and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // DC-IAM parameters (line, gap)
            select block, timestamp, tx_hash, order_index,
            case substr(location, length(location))
            when '0' then 'DC-IAM.ilks.line'
            when '1' then 'DC-IAM.ilks.gap'
            end as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            etl_hextoint(prev_value) / pow(10, 45) as from_value,
            etl_hextoint(curr_value) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
            location like '0[%' and
            substr(location, length(location)) in ('0', '1') and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // DC-IAM parameters (ttl)
            select block, timestamp, tx_hash, order_index,
            'DC-IAM.ilks.ttl'as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            etl_hextoint(right(prev_value, 12)) as from_value,
            etl_hextoint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc7bdd1f2b16447dcf3de045c4a039a60ec2f0ba3' and
            location like '0[%' and
            substr(location, length(location)) = '2' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // SPOTTER parameters (mat)
            select block, timestamp, tx_hash, order_index,
            'SPOTTER.ilks.mat' as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            etl_hextoint(prev_value) / pow(10, 27) as from_value,
            etl_hextoint(curr_value) / pow(10, 27) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x65c79fcb50ca1594b025960e539ed7a9a6d434a3' and
            location like '1[%.1' and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // JUG parameters (duty)
            select block, timestamp, tx_hash, order_index,
            'JUG.ilks.duty' as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            iff(etl_hextoint(prev_value) > 0, round(pow(etl_hextoint(prev_value) / pow(10, 27), 606024365), 4) - 1, 0)
            as from_value,
            iff(etl_hextoint(curr_value) > 0, round(pow(etl_hextoint(curr_value) / pow(10, 27), 606024365), 4) - 1, 0)
            as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x19c0976f590d67707e62397c87829d896dc0f1f1' and
            location like '1[%.0' and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // DOG parameters (chop, hole)
            select block, timestamp, tx_hash, order_index,
            case substr(location, length(location))
            when '1' then 'DOG.ilks.chop'
            when '2' then 'DOG.ilks.hole'
            end as parameter,
            maker.public.etl_hextostr(substr(location, 3, 42)) as ilk,
            case substr(location, length(location))
            when '1' then iff(etl_hextoint(prev_value) = 0, 0, etl_hextoint(prev_value) / pow(10, 18) - 1)
            else etl_hextoint(prev_value) / pow(10, 45)
            end as from_value,
            case substr(location, length(location))
            when '1' then iff(etl_hextoint(curr_value) = 0, 0, etl_hextoint(curr_value) / pow(10, 18) - 1)
            else etl_hextoint(curr_value) / pow(10, 45)
            end as to_value
            from edw_share.raw.storage_diffs
            where contract = '0x135954d155898d42c90d2a57824c690e0c7bef1b' and
            location like '1[%' and
            substr(location, length(location)) in ('1', '2') and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // CLIPPERs parameters (buf, tail, cusp)
            select d.block, timestamp, tx_hash, order_index,
            case substr(location, length(location))
            when '5' then 'CLIPPER.buf'
            when '6' then 'CLIPPER.tail'
            when '7' then 'CLIPPER.cusp'
            end as parameter,
            c.ilk,
            case substr(location, length(location))
            when '6' then etl_hextoint(prev_value)
            else etl_hextoint(prev_value) / pow(10, 27)
            end as from_value,
            case substr(location, length(location))
            when '6' then etl_hextoint(curr_value)
            else etl_hextoint(curr_value) / pow(10, 27)
            end as to_value
            from edw_share.raw.storage_diffs d, clippers c
            where d.contract = c.address and
            d.location in ('5', '6', '7') and
            d.block > {setup['start_block']} and block <= {setup['end_block']} and
            d.status

            union

            // CLIPPERs parameters (chip)
            select d.block, timestamp, tx_hash, order_index,
            'CLIPPER.chip' as parameter,
            c.ilk,
            etl_hextoint(right(prev_value, 16)) / pow(10, 18) as from_value,
            etl_hextoint(right(curr_value, 16)) / pow(10, 18) as to_value
            from edw_share.raw.storage_diffs d, clippers c
            where d.contract = c.address and
            d.location = '8' and
            from_value != to_value and
            d.block > {setup['start_block']} and block <= {setup['end_block']} and
            d.status

            union

            // CLIPPERs parameters (tip)
            select d.block, timestamp, tx_hash, order_index,
            'CLIPPER.tip' as parameter,
            c.ilk,
            etl_hextoint(substr(prev_value, 1, len(prev_value)-16)) / pow(10, 45) as from_value,
            etl_hextoint(substr(curr_value, 1, len(curr_value)-16)) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs d, clippers c
            where d.contract = c.address and
            d.location = '8' and
            from_value != to_value and
            d.block > {setup['start_block']} and block <= {setup['end_block']} and
            d.status

            union

            // VOW parameters (hump, sump, dump, bump)
            select block, timestamp, tx_hash, order_index,
            case location
            when '8' then 'VOW.dump'
            when '9' then 'VOW.sump'
            when '10' then 'VOW.bump'
            when '11' then 'VOW.hump'
            end as parameter,
            null as ilk,
            etl_hextoint(prev_value) / pow(10, 45) as from_value,
            etl_hextoint(curr_value) / pow(10, 45) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa950524441892a31ebddf91d3ceefa04bf454466' and
            location in ('8', '9', '10', '11') and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // FLAPPER parameters (beg)
            select block, timestamp, tx_hash, order_index,
            'FLAPPER.beg' as parameter,
            null as ilk,
            etl_hextoint(prev_value) / pow(10, 18) as from_value,
            etl_hextoint(curr_value) / pow(10, 18) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
            location = '4' and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // FLAPPER parameters (ttl)
            select block, timestamp, tx_hash, order_index,
            'FLAPPER.ttl' as parameter,
            null as ilk,
            etl_hextoint(right(prev_value, 12)) as from_value,
            etl_hextoint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' and
            location = '5' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // FLOPPER parameters (bed, pad)
            select block, timestamp, tx_hash, order_index,
            case location
            when '4' then 'FLOPPER.beg'
            when '5' then 'FLOPPER.pad'
            end as parameter,
            null as ilk,
            etl_hextoint(prev_value) / pow(10, 18) as from_value,
            etl_hextoint(curr_value) / pow(10, 18) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
            location in ('4', '5') and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            union

            // FLOPPER parameters (ttl)
            select block, timestamp, tx_hash, order_index,
            'FLOPPER.ttl' as parameter,
            null as ilk,
            etl_hextoint(right(prev_value, 12)) as from_value,
            etl_hextoint(right(curr_value, 12)) as to_value
            from edw_share.raw.storage_diffs
            where contract = '0xa41b6ef151e06da0e34b009b86e828308986736d' and
            location = '6' and
            from_value != to_value and
            block > {setup['start_block']} and block <= {setup['end_block']} and status

            ) p join edw_share.raw.transactions t on p.tx_hash = t.tx_hash
            order by block desc, order_index desc);
        """
    )

    sf.execute(
        f"""
            insert into maker.scheduler.parameters (load_id, start_block, end_block)
            values('{setup['load_id']}', {setup['start_block']}, {setup['end_block']});
        """
    )

    return