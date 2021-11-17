import snowflake.connector
from airflow.exceptions import AirflowFailException
from config import SNOWFLAKE_CONNECTION
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage


def _current_vaults(**setup):
    try:
        connection = snowflake.connector.connect(**SNOWFLAKE_CONNECTION)
        sf_transaction = connection.cursor()

        records = sf_transaction.execute(
            f"""
            SELECT *, {setup['end_block']}, '{setup['end_time']}'
            FROM {setup['db']}.INTERNAL.CURRENT_VAULTS
            ORDER BY vault;
        """
        ).fetchall()

        sf_transaction.execute(f"TRUNCATE table {setup['db']}.PUBLIC.CURRENT_VAULTS; ")

        pattern = _write_to_stage(sf_transaction, records, f"{setup['db']}.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf_transaction,
                f"{setup['db']}.staging.vaults_extracts",
                f"{setup['db']}.public.current_vaults",
                pattern,
            )
            _clear_stage(sf_transaction, f"{setup['db']}.staging.vaults_extracts", pattern)

    except:

        raise AirflowFailException("#ERROR: CURRENT_VAULTS not updated. Shutting down the process")

    return
