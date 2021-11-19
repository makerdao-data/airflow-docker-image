from operator import itemgetter

# import os
# import sys
# sys.path.append(os.environ.get('SYS_PATH'))
from dags.connectors.sf import sf
from dags.adapters.snowflake.stage import transaction_clear_stage, transaction_write_to_stage
from dags.adapters.snowflake.table import transaction_write_to_table


def _full_proxies_history(up, down, **setup):

    proxies_from_db = sf.execute(
        f"""
        select 
            to_varchar(load_id, 'YYYY-MM-DD HH:MI:SS') load_id, block, tx_index,
            to_varchar(timestamp, 'YYYY-MM-DD HH:MI:SS') timestamp, tx_hash,
            lower(cold), lower(hot), lower(proxy), action, breadcrumb,
            from_address, to_address, gas_used, gas_price
        from {setup['votes_db']}.internal.vote_proxies
        order by timestamp;
    """
    ).fetchall()

    full = proxies_from_db + up + down
    full_sorted = sorted(full, key=lambda x: x[1])

    latest = up + down
    latest_sorted = []
    if latest:
        latest_sorted = sorted(latest, key=itemgetter(1))
        pattern = transaction_write_to_stage(sf, latest_sorted, f"{setup['votes_db']}.staging.votes_extracts")
        if pattern:
            transaction_write_to_table(
                sf,
                f"{setup['votes_db']}.staging.votes_extracts",
                f"{setup['votes_db']}.internal.vote_proxies",
                pattern,
            )
            transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

    return latest_sorted, full_sorted
