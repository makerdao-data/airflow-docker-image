from datetime import timedelta
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.gcp import bq_query


def _pre_load_check(blocks=None, vat=None, manager=None, **setup):

    # Data Quality tests
    tests = [True]
    content = []
    setup['dataset'] = "bigquery-public-data.crypto_ethereum"

    b_count, b_min, b_timestamp = sf.execute(
        f"""
        SELECT count(distinct block), min(block), max(timestamp)
        FROM {setup['db']}.staging.blocks; """
    ).fetchone()

    b_max = 0
    for b in blocks:
        if b[1] > b_max:
            b_max = b[1]

    b_count = b_count + len(blocks)

    test = b_max - b_min == b_count - 1
    info = "PRE:: BLOCKS continuity check: %s" % ('passed' if test else 'failed')
    content.append(info)
    tests.append(test)

    uniq_VAT_operations = []
    for i in vat:
        if i not in uniq_VAT_operations:
            uniq_VAT_operations.append(i)

    test = len(vat) == len(uniq_VAT_operations)
    info = "PRE:: VAT uniqueness check: %s" % ('passed' if test else 'failed')
    content.append(info)
    tests.append(test)

    b_date = b_timestamp
    b_date = b_date - timedelta(days=1)
    b_date = b_date.__str__()[:10]

    data = ','.join(["'%s'" % sig for sig in setup['vat_abi']])

    count1, = sf.execute(
        f"""SELECT count(*)
        FROM {setup['db']}.staging.vat
        WHERE date(timestamp) >= '{b_date}'; """
    ).fetchone()

    count1 = count1 + len(vat)

    count2 = bq_query(
        f"""SELECT count(*)
        FROM `{setup['dataset']}.traces`
        WHERE block_number < {b_max}
            AND date(block_timestamp) >= '{b_date}'
            AND to_address = '{setup['vat_address']}'; """
    )[0][0]

    test = count1 == count2
    info = "PRE:: VAT daily count check: %s" % ('passed' if test else 'failed')
    content.append(info)
    tests.append(test)

    uniq_CDP_Manager_operations = []
    for i in manager:
        if i not in uniq_CDP_Manager_operations:
            uniq_CDP_Manager_operations.append(i)

    test = len(manager) == len(uniq_CDP_Manager_operations)
    info = "PRE:: CDP uniqueness check: %s" % ('passed' if test else 'failed')
    content.append(info)
    tests.append(test)

    manager_input = ','.join(["'%s'" % sig for sig in setup['manager_abi']])

    count1, = sf.execute(
        f"""SELECT count(*)
        FROM {setup['db']}.staging.manager
        WHERE date(timestamp) >= '{b_date}'
            AND block <= {b_max}; """
    ).fetchone()

    count2 = bq_query(
        f"""SELECT count(*)
        FROM `{setup['dataset']}.traces`
        WHERE block_number <= {b_max}
            AND date(block_timestamp) >= '{b_date}'
            AND to_address = '{setup['manager_address']}'
            AND substr(input, 1, 10) IN ({manager_input}); """
    )[0][0]

    test = count1 == count2
    info = "PRE:: CDP daily count check: %s" % ('passed' if test else 'failed')
    content.append(info)
    tests.append(test)

    return
