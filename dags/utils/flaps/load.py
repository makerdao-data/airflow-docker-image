from dags.connectors.sf import sf


def _load(**setup):

    sf.execute(
        """
            create or replace function maker.public.flap_etl_hextoint (s string)
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

    q = f"""
            insert into maker.public.flaps
            select '{setup['load_id']}', a.timestamp, a.block, a.tx_hash, a.status, a.order_index, a.action, a.id, a.lot, a.bid, a.guy, t.from_address, t.to_address
            from (
                select timestamp, block, log_index, tx_hash, status, order_index,
                    case substr(topic0, 1, 10)
                    when '0xe6dde59c' then 'Kick'
                    when '0x4b43ed12' then 'Tend'
                    when '0xc959c42b' then 'Deal'
                    end as action,
                    case substr(topic0, 1, 10)
                    when '0xe6dde59c' then maker.public.flap_etl_hextoint(substr(log_data, 1, 66))
                    when '0x4b43ed12' then maker.public.flap_etl_hextoint(topic2)
                    when '0xc959c42b' then maker.public.flap_etl_hextoint(topic2)
                    end as id,
                    case substr(topic0, 1, 10)
                    when '0xe6dde59c' then maker.public.flap_etl_hextoint(substr(log_data, 67, 64)) / pow(10, 45)
                    when '0x4b43ed12' then maker.public.flap_etl_hextoint(topic3) / pow(10, 45)
                    when '0xc959c42b' then null
                    end as lot,
                    case substr(topic0, 1, 10)
                    when '0xe6dde59c' then maker.public.flap_etl_hextoint(substr(log_data, 131, 64)) / pow(10, 45)
                    when '0x4b43ed12' then maker.public.flap_etl_hextoint(substr(log_data, 139+2*64, 64)) / pow(10, 18)
                    when '0xc959c42b' then null
                    end as bid,
                    case substr(topic0, 1, 10)
                    when '0xe6dde59c' then null
                    when '0x4b43ed12' then concat('0x', right(topic1, 40))
                    when '0xc959c42b' then concat('0x', right(topic1, 40))
                    end as guy
                from edw_share.raw.events
                where contract = '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d'
                and substr(topic0, 1, 10) in ('0x4b43ed12', '0xc959c42b', '0xe6dde59c')
                and block > {setup['start_block']} and block <= {setup['end_block']}
            ) a join edw_share.raw.transactions t on a.tx_hash = t.tx_hash
            order by a.block, log_index;
        """

    sf.execute(q)

    sf.execute(
        f"""
            insert into maker.scheduler.flaps (load_id, start_block, end_block)
            values('{setup['load_id']}', {setup['start_block']}, {setup['end_block']});
        """
    )

    return
