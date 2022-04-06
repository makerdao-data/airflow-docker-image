import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dags.connectors.sf import sf
from dags.utils.history.mkr_history import update_mkr_history

sys.path.append('/opt/airflow/')

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
    schedule_interval='0 * * * *',
    start_date=datetime(2022, 4, 5, 0),
    max_active_runs=1,
    catchup=False,
)
def prod_mkr_history():

    @task()
    def update_mkr_tx_history():

        update_mkr_history(sf)

        return

    update_mkr_tx_history()


prod_mkr_history = prod_mkr_history()
