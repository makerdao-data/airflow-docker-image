from datetime import datetime
import snowflake.connector
from config import SNOWFLAKE_CONNECTION
from airflow.exceptions import AirflowFailException
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_table, _clear_stage


def _load(blocks, vat, vat_operations, manager, manager_operations, ratios, rates, prices, vaults_operations, public_vaults, **setup):

    connection = snowflake.connector.connect(**SNOWFLAKE_CONNECTION)
    sf_transaction = connection.cursor()

    try:
        sf_transaction.execute("BEGIN TRANSACTION; ")

        if blocks:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.staging.blocks",
                blocks,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", blocks)

        del blocks

        if vat:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.staging.vat",
                vat,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", vat)

        del vat

        if manager:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.staging.manager",
                manager,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", manager)

        del manager

        if ratios:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.internal.mats",
                ratios,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", ratios)

        del ratios

        if rates:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.internal.rates",
                rates,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", rates)

        del rates

        if prices:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.internal.prices",
                prices,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", prices)

        del prices

        if vaults_operations:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.operations.vault",
                vaults_operations,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", vaults_operations)

        del vaults_operations

        if vat_operations:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.operations.vat",
                vat_operations,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", vat_operations)

        del vat_operations

        if manager_operations:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.operations.manager",
                manager_operations,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", manager_operations)

        del manager_operations

        if public_vaults:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.public.vaults",
                public_vaults,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", public_vaults)

        del public_vaults


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
