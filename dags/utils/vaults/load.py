from datetime import datetime
from decimal import Decimal
import snowflake.connector
from config import SNOWFLAKE_CONNECTION
from airflow.exceptions import AirflowFailException
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage


def _load(blocks, vat, vat_operations, manager, manager_operations, ratios, rates, prices, vaults_operations, public_vaults, **setup):

    ### START TRANSACTION

    print('#INFO: STARTING TRANSACTION')

    connection = snowflake.connector.connect(**SNOWFLAKE_CONNECTION)
    sf_transaction = connection.cursor()

    try:
        print('BEGIN TRANSACTION')
        sf_transaction.execute("BEGIN TRANSACTION; ")

        print('NEW BLOCKS')
        records = []
        for i in blocks:
            temp = i
            temp[5] = Decimal(i[5])
            records.append(temp)

        pattern = _write_to_stage(sf_transaction, records, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.staging.blocks",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del blocks
        del records

        print('VAT OPERATIONS')
        records = []
        for i in vat:
            temp = i
            temp[7] = Decimal(i[7])
            records.append(temp)

        pattern = _write_to_stage(sf_transaction, records, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.staging.vat",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del vat
        del records

        print('MCD OPERATIONS')
        records = []
        for i in manager:
            temp = i
            temp[7] = Decimal(i[7])
            records.append(temp)

        pattern = _write_to_stage(sf_transaction, records, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.staging.manager",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del manager
        del records

        print('MATS')
        pattern = _write_to_stage(sf_transaction, ratios, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.internal.mats",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del ratios

        print('RATES')
        pattern = _write_to_stage(sf_transaction, rates, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.internal.rates",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del rates

        print('MARKET AND OSM PRICES - UPDATED')
        records = []
        for i in prices:
            temp = i
            if type(i[4]) == str:
                temp[4] = Decimal(i[4])
            if type(i[5]) == str:
                temp[5] = Decimal(i[5])
            records.append(temp)

        pattern = _write_to_stage(sf_transaction, records, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.internal.prices",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del prices
        del records

        print('VAULTS OPERATIONS')
        pattern = _write_to_stage(sf_transaction, vaults_operations, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.operations.vault",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del vaults_operations

        print('VAT OPERATIONS')
        pattern = _write_to_stage(sf_transaction, vat_operations, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.operations.vat",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del vat_operations

        print('MANAGER OPERATIONS')
        pattern = _write_to_stage(sf_transaction, manager_operations, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.operations.manager",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del manager_operations

        print('PUBLIC VAULTS')

        pattern = _write_to_stage(sf_transaction, public_vaults, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.public.vaults",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

        del public_vaults

        print('PROCESS DETAILS')

        proc_end = datetime.utcnow().__str__()[:19]
        sf_transaction.execute(
            f"""
            INSERT INTO {setup['db']}.INTERNAL.{setup['scheduler']} (load_id, proc_start, start_block, end_block, proc_end, status)
            VALUES ('{setup['load_id']}', '{setup['start_time']}', {setup['start_block'] +1}, {setup['end_block']}, '{proc_end}', 1);
        """
        )

        sf_transaction.execute("COMMIT; ")
        sf_transaction.close()

    except:

        sf_transaction.execute("ROLLBACK; ")
        sf_transaction.close()
        raise AirflowFailException("#ERROR: Data NOT loaded. Shutting down the process")

    return
