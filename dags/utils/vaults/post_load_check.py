from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.gcp import bq_query


def _post_load_check(**setup):

    # Data Quality tests
    tests = [True]

    b_count, b_min, b_max, b_date = sf.execute(
        f"""
        SELECT count(distinct block), min(block), max(block), max(date(timestamp))
        FROM {setup['db']}.staging.blocks;
    """
    ).fetchone()
    test = b_max - b_min == b_count - 1
    tests.append(test)

    print(f'b_min: {b_min}')
    print(f'b_max: {b_max}')
    print(f'b_date: {b_date}')

    count1, count2 = sf.execute(
        f"""
        SELECT count(*), count(distinct block, tx_index, breadcrumb)
        FROM {setup['db']}.staging.vat;
    """
    ).fetchone()
    test = count1 == count2
    tests.append(test)

    print('test 1')
    print(f'count1: {count1}')
    print(f'count2: {count2}')

    count1, = sf.execute(
        f"""
        SELECT count(*)
        FROM {setup['db']}.staging.vat
        WHERE block <= {b_max}
            and date(timestamp) >= '{b_date - timedelta(days=1)}';
    """
    ).fetchone()

    count2 = bq_query(
        f"""
        SELECT count(*)
        FROM `bigquery-public-data.crypto_ethereum.traces`
        WHERE block_number <= {b_max}
            AND date(block_timestamp) >= '{b_date - timedelta(days=1)}'
            AND to_address = '{'0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'}'
            AND substr(input, 1, 10) = '0x1a0b287e' AND status = 1;
    """
    )[0][0]

    test = count1 == count2
    tests.append(test)

    print('test 2')
    print(f'count1: {count1}')
    print(f'count2: {count2}')

    count1, count2 = sf.execute(
        f"""
        SELECT count(*), count(distinct block, tx_index, breadcrumb)
        FROM {setup['db']}.staging.manager;
    """
    ).fetchone()
    test = count1 == count2
    tests.append(test)

    print('test 3')
    print(f'count1: {count1}')
    print(f'count2: {count2}')

    count1, = sf.execute(
        f"""
        SELECT count(*)
        FROM {setup['db']}.staging.manager
        WHERE block <= {b_max}
            AND date(timestamp) >= '{b_date - timedelta(days=1)}';
    """
    ).fetchone()

    count2 = bq_query(
        f"""
        SELECT count(*)
        FROM `{setup['dataset']}.traces`
        WHERE block_number <= {b_max}
            AND date(block_timestamp) >= '{b_date - timedelta(days=1)}'
            AND to_address = '{setup['manager_address']}'
            AND substr(input, 1, 10) in ({','.join(["'%s'" % sig for sig in setup['manager_abi']])})
    """
    )[0][0]
    test = count1 == count2
    tests.append(test)

    print('test 4')
    print(f'count1: {count1}')
    print(f'count2: {count2}')

    count1, count2 = sf.execute(
        f"""
        SELECT count(*), count(distinct order_index, block, vault, operation, dcollateral)
        FROM {setup['db']}.public.vaults;
    """
    ).fetchone()
    test = count1 == count2
    tests.append(test)

    print('test 5')
    print(f'count1: {count1}')
    print(f'count2: {count2}')



    count1 = sf.execute(f"""
        select count(*)
        from mcd.staging.vat
        where date(timestamp) >= '{setup['start_time'][:10]}'
            and function is not null
            and block <= {setup['end_block']};
    """
    ).fetchone()[0]

    count2 = bq_query(f"""
        SELECT count(*)
        FROM `bigquery-public-data.crypto_ethereum.traces`
        WHERE DATE(block_timestamp) >= '{setup['start_time'][:10]}'
            and block_number <= {setup['end_block']}
            and to_address = lower('0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b')
            and substr(input, 1, 10) in ('0x1a0b287e','0x29ae8114','0x3b663195','0x6111be2e','0x65fae35e','0x69245009','0x76088703','0x7bab3f40','0x7cdd3fde','0x870c616d','0x9c52a7f1','0xa3b22fc4','0xb65337df','0xbb35783b','0xdc4d20fa','0xf24e23eb','0xf37ac61c');
    """
    )[0][0]

    test = count1 == count2
    tests.append(test)

    print('test 6')
    print(f'count1: {count1}')
    print(f'count2: {count2}')
    if not test:

        raise AirflowFailException(f'WARNING: {count2 - count1} VAT OPERATIONS ARE MISSING')

    return
