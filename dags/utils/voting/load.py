from decimal import Decimal
from datetime import datetime
from airflow.exceptions import AirflowFailException
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.sf import clear_stage
from dags.adapters.snowflake.stage import transaction_clear_stage, transaction_write_to_stage
from dags.adapters.snowflake.table import transaction_write_to_table


def _load(chief, polls, api_polls, executives, votes, operations, **setup):

    try:

        clear_stage(f"{setup['votes_db']}.staging.votes_extracts")

        sf.execute("BEGIN TRANSACTION; ")

        c = []
        for i in chief:
            temp = i
            temp[7] = Decimal(i[7])
            c.append(temp)

        if c:
            pattern = transaction_write_to_stage(sf, c, f"{setup['votes_db']}.staging.votes_extracts")
            if pattern:
                transaction_write_to_table(
                    sf,
                    f"{setup['votes_db']}.staging.votes_extracts",
                    f"{setup['votes_db']}.staging.chief",
                    pattern,
                )
                transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        p = []
        for i in polls:
            temp = i
            temp[7] = Decimal(i[7])
            p.append(temp)

        if p:
            pattern = transaction_write_to_stage(sf, p, f"{setup['votes_db']}.staging.votes_extracts")
            if pattern:
                transaction_write_to_table(
                    sf,
                    f"{setup['votes_db']}.staging.votes_extracts",
                    f"{setup['votes_db']}.staging.polls",
                    pattern,
                )
                transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        if api_polls:

            pattern = transaction_write_to_stage(sf, api_polls, f"{setup['votes_db']}.staging.votes_extracts")
            if pattern:
                transaction_write_to_table(
                    sf,
                    f"{setup['votes_db']}.staging.votes_extracts",
                    f"{setup['votes_db']}.internal.yays",
                    pattern,
                )
                transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        if executives:

            pattern = transaction_write_to_stage(
                sf, executives, f"{setup['votes_db']}.staging.votes_extracts"
            )
            if pattern:
                transaction_write_to_table(
                    sf,
                    f"{setup['votes_db']}.staging.votes_extracts",
                    f"{setup['votes_db']}.internal.yays",
                    pattern,
                )
                transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        if operations:
            pattern = transaction_write_to_stage(
                sf, operations, f"{setup['votes_db']}.staging.votes_extracts"
            )
            if pattern:
                transaction_write_to_table(
                    sf,
                    f"{setup['votes_db']}.staging.votes_extracts",
                    f"{setup['votes_db']}.internal.gov_operations",
                    pattern,
                )
                transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        if votes:
            pattern = transaction_write_to_stage(sf, votes, f"{setup['votes_db']}.staging.votes_extracts")
            if pattern:
                transaction_write_to_table(
                    sf,
                    f"{setup['votes_db']}.staging.votes_extracts",
                    f"{setup['votes_db']}.public.votes",
                    pattern,
                )
                transaction_clear_stage(sf, f"{setup['votes_db']}.staging.votes_extracts", pattern)

        sf.execute(
            f"""
            INSERT INTO {setup['votes_db']}.internal.{setup['votes_scheduler']}(LOAD_ID, START_BLOCK, END_BLOCK, END_TIMESTAMP, STATUS)
            VALUES ('{setup['load_id']}', {setup['start_block']} + 1, {setup['end_block']}, '{datetime.utcnow().__str__()[:19]}', 1);
        """
        )

        sf.execute("COMMIT; ")

    except Exception as e:
        sf.execute("ROLLBACK; ")
        raise AirflowFailException("FATAL: Loading data failed.")

    return True
