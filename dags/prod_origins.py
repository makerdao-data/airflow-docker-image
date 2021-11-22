from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
sys.path.append('/opt/airflow/')
from dags.utils.origins.get_origins import _get_origins


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": ["piotr.m.klis@gmail.com"],
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
# [END default_args]



# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='0 4 * * *',
    start_date=datetime(2021, 11, 22, 12),
    max_active_runs=1,
    catchup=False,
)
def prod_origins_load():

    @task()
    def workflow():

        _get_origins()


    workflow()
    

prod_origins_load = prod_origins_load()
