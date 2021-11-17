from airflow.exceptions import AirflowFailException
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf


def _update_current_voters_table(**setup):

    try:

        sf.execute("BEGIN TRANSACTION; ")

        sf.execute(f"""select count(*) from {setup['votes_db']}.PUBLIC.CURRENT_VOTERS; """)
        current_voters = sf.fetchone()
        if current_voters[0] != 0:
            sf.execute(f"""truncate table {setup['votes_db']}.PUBLIC.CURRENT_VOTERS; """)
        sf.execute(
            f"""insert into {setup['votes_db']}.PUBLIC.CURRENT_VOTERS
                        select *, {setup['end_block']}, '{setup['end_time']}' from {setup['votes_db']}.INTERNAL.CURRENT_VOTERS
                        order by stake desc; """
        )
        sf.execute("COMMIT; ")

    except Exception as e:

        sf.execute("ROLLBACK; ")
        raise AirflowFailException("FATAL: Table CURRENT_VOTERS not updated.")

    return True
