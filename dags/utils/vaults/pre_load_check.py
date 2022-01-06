from datetime import datetime, timedelta
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

    if not b_count:
        b_count = 8928151
        b_min = 8928151
        b_timestamp = datetime(2019, 11, 13)

    b_max = 0
    blocks_counter = 0
    for b in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11 
        from @mcd.staging.vaults_extracts/{blocks} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():
        if int(b[1]) > b_max:
            b_max = int(b[1])
            blocks_counter += 1

    b_count = b_count + blocks_counter

    test = b_max - b_min == b_count - 1
    info = "PRE:: BLOCKS continuity check: %s" % ('passed' if test else 'failed')
    content.append(info)
    tests.append(test)

    vat_len = []
    uniq_VAT_operations = []
    for i in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16   
        from @mcd.staging.vaults_extracts/{vat} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():
        vat_len.append(i)
        if i not in uniq_VAT_operations:
            uniq_VAT_operations.append(i)

    test = len(vat_len) == len(uniq_VAT_operations)
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

    manager_len = []
    uniq_CDP_Manager_operations = []
    for i in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16   
        from @mcd.staging.vaults_extracts/{manager} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():
        manager_len.append(i)
        if i not in uniq_CDP_Manager_operations:
            uniq_CDP_Manager_operations.append(i)

    test = len(manager_len) == len(uniq_CDP_Manager_operations)
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
