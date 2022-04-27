import sys
from datetime import datetime, timedelta
import snowflake.connector

sys.path.append('/opt/airflow/')

from airflow.decorators import dag, task
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

from config import SNOWFLAKE_CONNECTION

SNOWFLAKE_CONNECTION['database'] = 'MAKER'
SNOWFLAKE_CONNECTION['schema'] = 'BALANCES'

conn = snowflake.connector.connect(**SNOWFLAKE_CONNECTION)

# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2022, 3, 22, 10),
    max_active_runs=1,
    catchup=False,
)
def prod_bals():

    @task()
    def update_dai_balances():

        update_token_balances('DAI', 10**-18, conn)

        return

    @task()
    def update_mkr_balances():

        update_token_balances('MKR', 10**-18, conn)

        return

    update_dai_balances()
    update_mkr_balances()


prod_bals = prod_bals()
