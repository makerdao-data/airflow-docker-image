"""
Dag to update trackers, currerntly vault and governance.
.env file required in current directory.
"""
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from trackers.gov_updater import update_gov_data
from trackers.vault_updater import sf_connect, update_vault_data


sys.path.append('/opt/airflow/')

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
    schedule_interval='*/20 * * * *',
    start_date=datetime(2022, 3, 16, 0),
    max_active_runs=1,
    catchup=False,
)
def update_trackers(sf) -> None:

    @task()
    def workflow() -> None:
        update_vault_data(sf)
        update_gov_data(sf)


sf = sf_connect()
update_trackers = update_trackers(sf)
