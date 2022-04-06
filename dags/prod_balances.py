import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task

sys.path.append('/opt/airflow/')

from dags.connectors.sf import connection as conn
from dags.utils.balances.update_tkn_balances import update_token_balances


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": "airflow@data.makerdao.network",
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
# [END default_args]


# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='30 3 * * *',
    start_date=datetime(2022, 4, 6, 10),
    max_active_runs=1,
    catchup=False,
)
def prod_bals():

    @task()
    def update_dai_balances():

        update_token_balances('DAI', conn)

        return

    @task()
    def update_mkr_balances():

        update_token_balances('MKR', conn)

        return

    update_dai_balances()
    update_mkr_balances()


prod_bals = prod_bals()
