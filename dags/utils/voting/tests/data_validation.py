from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.environ.get('SYS_PATH'))
from dags.connectors.sf import sf
from dags.connectors.gcp import bq_query
from dags.utils.voting.tooling.get_executives import get_execs


def _data_validation(api_polls, execs, **setup):
    # Data Quality tests

    b_count, b_min, b_max, b_date = sf.execute(
        """
        SELECT count(distinct block), min(block), max(block), max(date(timestamp))
        FROM mcd.staging.blocks; """
    ).fetchone()

    tests = [True]

    # chief operations test
    count1 = sf.execute(
        f"""
        select count(*)
        from {setup['votes_db']}.staging.chief
        where date(timestamp) >= '{b_date - timedelta(days=1)}'; """
    ).fetchone()

    count2 = bq_query(
        f"""
        SELECT count(*)
        FROM `{setup['dataset']}.traces`
        WHERE block_number < {b_max}
            AND date(block_timestamp) >= '{b_date - timedelta(days=1)}'
            AND to_address in ({','.join(["'%s'" % address for address in setup['chief_addresses']])})
            AND substr(input, 1, 10) in ({','.join(["'%s'" % sig for sig in setup['chief_abi']])}); """
    )[0][0]

    test = count1[0] == count2
    tests.append(test)

    # polling test
    count1 = sf.execute(
        f"""
        select count(*)
        from {setup['votes_db']}.staging.polls
        where date(timestamp) >= '{b_date - timedelta(days=1)}' ;"""
    ).fetchone()

    count2 = bq_query(
        f"""
        SELECT count(*)
        FROM `{setup['dataset']}.traces`
        WHERE block_number < {b_max}
            AND date(block_timestamp) >= '{b_date - timedelta(days=1)}'
            AND to_address = '{setup['polls_address']}'
            AND substr(input, 1, 10) in ({','.join(["'%s'" % sig for sig in setup['polls_abi']])}); """
    )[0][0]

    count3 = bq_query(
        f"""
        SELECT count(*)
        FROM `{setup['dataset']}.traces`
        WHERE block_number < {b_max}
            AND date(block_timestamp) >= '{b_date - timedelta(days=1)}'
            AND to_address = '{setup['new_polls_address']}'
            AND substr(input, 1, 10) in ({','.join(["'%s'" % sig for sig in setup['new_polls_abi']])}); """
    )[0][0]

    test = count1[0] == count2 + count3
    tests.append(test)

    yays_poll = sf.execute(
        f"""select code from {setup['votes_db']}.internal.yays where type = 'poll'; """
    ).fetchall()

    y_polls = []
    for i in api_polls:
        if i[1] in yays_poll:
            y_polls.append(True)
        else:
            y_polls.append(False)

    test = all(y_polls)
    tests.append(test)

    yays_exec = sf.execute(
        f"""select code from {setup['votes_db']}.internal.yays where type = 'executive'; """
    ).fetchall()
    _yays_execs = []
    [_yays_execs.append(i[0].lower().strip()) for i in yays_exec]

    y_execs = []
    for i in execs:

        if i[1].lower().strip() not in _yays_execs and i['address'].lower().strip() not in [
            '0x0000000000000000000000000000000000000000',
            '0x02fc38369890aff2ec94b28863ae0dacdb2dbae3',
        ]:
            y_execs.append(False)
        else:
            y_execs.append(True)

    test = all(y_execs)
    tests.append(test)

    print('------------------------------')
    print("Data Quality check: %s" % ('passed' if all(tests) else 'failed'))

    return True
