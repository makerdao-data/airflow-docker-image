import json
from ast import literal_eval
from random import randint
from dags.connectors.sf import sf
from dags.connectors.gcp import bq_query
from dags.utils.general import balance_of
from dags.adapters.snowflake.stage import transaction_clear_stage, transaction_write_to_stage
from dags.adapters.snowflake.table import transaction_write_to_table


def _count_votes(**setup):

    polls = sf.execute(
        f"""
        select code, end_timestamp
        from {setup['votes_db']}.internal.yays
        where type = 'poll'
            and block_ended is null
            and results is null;
    """
    ).fetchall()

    max_block, max_timestamp = sf.execute(
        f"""
        select max(block), max(timestamp)
        from mcd.staging.blocks
        where timestamp <= '{setup['end_time']}';
    """
    ).fetchone()

    for yay, end_timestamp in polls:

        if max_timestamp >= end_timestamp:
            end_block = bq_query(
                f"""
                select max(number)
                from `bigquery-public-data.crypto_ethereum.blocks`
                where timestamp <= '{end_timestamp}' and date(timestamp) = '{end_timestamp.__str__()[:10]}';
            """
            )

            if end_block[0][0]:
                sf.execute(
                    f"""
                    update {setup['votes_db']}.internal.yays
                    set block_ended = {end_block[0][0]}
                    where code = '{yay}' and end_timestamp = '{end_timestamp.__str__()[:19]}';
                """
                )

    polls = sf.execute(
        f"""
        select code, block_ended, end_timestamp, options
        from {setup['votes_db']}.internal.yays
        where type = 'poll'
            and start_timestamp <= '{setup['load_id']}'
            and block_ended is not null
            and results is null;
    """
    ).fetchall()

    for yay, end_block, end_timestamp, options in polls:

        voters = sf.execute(
            f"""select distinct(voter) from {setup['votes_db']}.public.votes where yay = '{yay}' and timestamp <= '{end_timestamp}'; """
        ).fetchall()
        final_votes = dict()

        for v in voters:

            voter, option = sf.execute(
                f"""
                select voter, option
                from {setup['votes_db']}.public.votes
                where lower(voter) = lower('{v[0].lower()}')
                    and yay = '{yay}'
                    and timestamp <= '{end_timestamp}'
                    and operation = 'CHOOSE'
                order by timestamp desc;
            """
            ).fetchone()

            final_votes.setdefault(option, [])
            final_votes[option].append(voter)

        all_proxy_history = sf.execute(
            f"""
            select load_id, block, tx_index, timestamp, tx_hash, lower(cold), lower(hot), lower(proxy), action
            from {setup['votes_db']}.internal.vote_proxies
            where timestamp <= '{end_timestamp}'
            order by timestamp;
        """
        ).fetchall()

        proxies = dict()
        for load_id, block, tx_index, timestamp, tx_hash, cold, hot, proxy, action in all_proxy_history:

            if action == 'create':
                proxies[hot] = dict(cold=cold, proxy=proxy)
            elif action == 'break':
                try:
                    proxies.pop(hot)
                except Exception as e:
                    print(e)
                    print(hot)
            else:
                pass

        poll_summary = dict()
        [poll_summary.setdefault(option, 0) for option in literal_eval(options)]

        for option in final_votes:

            calc = 0
            voters = final_votes[option]

            final_votes_records = list()
            voting_power = 0

            for v in voters:

                s = 0
                cold_wallet = 0
                hot_wallet_MKR = 0

                # STAKED FROM DB
                staked = sf.execute(
                    f"""
                    select sum(dstake)
                    from {setup['votes_db']}.public.votes
                    where lower(voter) = lower('{v}')
                        and timestamp <= '{end_timestamp}'
                        and operation != 'FINAL_CHOICE';
                """
                ).fetchone()

                if staked[0]:
                    s = staked[0]

                if not end_block:
                    end_block = max_block

                hot_wallet_MKR = balance_of('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', v, end_block)
                if not hot_wallet_MKR:
                    hot_wallet_MKR = 0

                vote_proxy = None
                if v in proxies:
                    vote_proxy = proxies[v]['proxy']
                    cold = proxies[v]['cold']

                if vote_proxy:
                    # ... plus MKRs amount in cold wallet
                    if cold.lower() != v.lower():
                        cold_wallet = balance_of(
                            '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', cold, end_block
                        )

                voting_power = s + hot_wallet_MKR + cold_wallet

                calc += voting_power

                r = [
                    setup['load_id'],
                    str(end_block).zfill(9) + '_' + '000' + '_' + '000',
                    end_block,
                    end_timestamp,
                    None,
                    None,
                    v,
                    vote_proxy,
                    0,
                    'FINAL_CHOICE',
                    yay,
                    option,
                    voting_power,
                    False,
                    None,
                    0,
                    0,
                ]

                final_votes_records.append(r)

            poll_summary[option] = calc

            if end_block != max_block:
                pattern = transaction_write_to_stage(
                    sf, final_votes_records, f"{setup['votes_db']}.staging.votes_extracts"
                )
                if pattern:
                    transaction_write_to_table(
                        sf,
                        f"{setup['votes_db']}.staging.votes_extracts",
                        f"{setup['votes_db']}.public.votes",
                        pattern,
                    )
                    transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        result = f"""
            update {setup['votes_db']}.internal.yays
            set results = (select parse_json('{json.dumps(poll_summary)}'))
            where code = '{yay}' and block_ended = {end_block};
        """

        sf.execute(result)

    return True
