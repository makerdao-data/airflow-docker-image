from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os, sys

sys.path.append('/opt/airflow/')

from dags.connectors.sf import sf
from dags.utils.history.table_history import update_table_history

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": ["airflow@data.makerdao.network"],
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
# [END default_args]


# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2022, 3, 17, 0),
    max_active_runs=1,
    catchup=False,
)
def prod_table_history():

    @task()
    def update():

        update_table_history(sf)

        return

    update()


prod_history = prod_table_history()
