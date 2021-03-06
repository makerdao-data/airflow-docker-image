from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys

sys.path.append('/opt/airflow/')

from dags.connectors.sf import sf
from dags.utils.history.history import _history

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": ["piotr.m.klis@gmail.com", "airflow@data.makerdao.network"],
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
# [END default_args]


# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2022, 2, 18, 1),
    max_active_runs=1,
    catchup=False,
)
def prod_history():

    @task()
    def history():

        _history()

        return

    history()

prod_history = prod_history()
