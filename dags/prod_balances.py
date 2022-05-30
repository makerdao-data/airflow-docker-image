import sys
from datetime import datetime, timedelta
import snowflake.connector

sys.path.append('/opt/airflow/')

from airflow.decorators import dag, task
from dags.utils.balances.dai import update_dai_balances
from dags.utils.balances.mkr import update_mkr_balances

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
    schedule_interval='30 2 * * *',
    start_date=datetime(2022, 5, 30, 6),
    max_active_runs=1,
    catchup=False,
)
def prod_bals():

    @task()
    def dai_balances():

        update_dai_balances()

        return

    @task()
    def mkr_balances():

        update_mkr_balances()

        return

    dai_balances()
    mkr_balances()


prod_bals = prod_bals()
