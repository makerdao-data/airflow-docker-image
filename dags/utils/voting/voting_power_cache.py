from dags.connectors.sf import sf
from dags.utils.general import balance_of
from dags.adapters.snowflake.stage import transaction_clear_stage, transaction_write_to_stage
from dags.adapters.snowflake.table import transaction_write_to_table


ABI = """[{"constant": true,"inputs": [],"name": "decimals","outputs": [{"internalType": "uint8","name": "","type": "uint8"}],
           "payable": false,"stateMutability": "view","type": "function"},
          {"constant": true,"inputs": [],"name": "symbol","outputs": [{"internalType": "string","name": "","type": "string"}],
           "payable": false,"stateMutability": "view","type": "function"},
          {"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
           "payable":false,"stateMutability":"view","type":"function"}]"""


def _voting_power_cache(**setup):

    unfinished_polls = sf.execute(
        f"""
        select code
        from {setup['votes_db']}.internal.yays
        where type = 'poll' and end_timestamp > '{setup['load_id']}';
    """
    ).fetchall()

    voters = list()

    for code in unfinished_polls:

        curr_voters = sf.execute(
            f"""
            select voter
            from {setup['votes_db']}.public.votes
            where operation = 'CHOOSE' and yay = '{code[0]}';
        """
        ).fetchall()

        voters += curr_voters

    distinct_voters = list()
    for voter in voters:
        if voter[0] not in distinct_voters:
            distinct_voters.append(voter[0])

    all_proxy_history = sf.execute(
        f"""
            select load_id, block, tx_index, timestamp, tx_hash, lower(cold), lower(hot), lower(proxy), action
            from {setup['votes_db']}.internal.stg_proxies
            where timestamp <= '{setup['load_id']}'
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

    voting_power_cache = list()

    latest_block = sf.execute(
        f"""
        select max(block)
        from {setup['vaults_db']}.staging.blocks
        where timestamp < '{setup['load_id']}'
    """
    ).fetchone()[0]

    for v in distinct_voters:

        hot_wallet_MKR = balance_of('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', v, latest_block)
        if not hot_wallet_MKR:
            hot_wallet_MKR = 0

        vote_proxy = None
        if v in proxies:
            vote_proxy = proxies[v]['proxy']
            cold = proxies[v]['cold']

        cold_wallet_MKR = 0
        if vote_proxy:
            # ... plus MKRs amount in cold wallet
            if cold.lower() != v.lower():
                cold_wallet_MKR = balance_of('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', cold, latest_block)

        voting_power_cache.append([v, hot_wallet_MKR + cold_wallet_MKR])

    sf.execute(f"""truncate {setup['votes_db']}.internal.voting_power; """)
    f = transaction_write_to_stage(
        sf, voting_power_cache, f"""{setup['votes_db']}.staging.voting_power_cache"""
    )
    if f:
        transaction_write_to_table(
            sf,
            f"""{setup['votes_db']}.staging.voting_power_cache""",
            f"""{setup['votes_db']}.internal.voting_power""",
            f,
        )
        transaction_clear_stage(sf, f"""{setup['votes_db']}.staging.voting_power_cache""", f)

    return True
