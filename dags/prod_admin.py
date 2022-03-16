from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
sys.path.append('/opt/airflow/')
from dags.utils.admins.get_admins import _get_admins


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
    schedule_interval='0 2 * * *',
    start_date=datetime(2022, 2, 17, 0),
    max_active_runs=1,
    catchup=False,
)
def prod_admin_load():

    @task()
    def workflow():

        _get_admins()


    workflow()


prod_admin_load = prod_admin_load()
