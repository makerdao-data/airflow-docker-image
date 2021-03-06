from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os, sys

sys.path.append('/opt/airflow/')

from dags.connectors.sf import connection as engine
from dags.connectors.chain import chain
from dags.utils.parameters.setup import _setup
from dags.utils.parameters.load import _load

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
    schedule_interval='15 */6 * * *',
    start_date=datetime(2022, 2, 17, 10),
    max_active_runs=1,
    catchup=False,
)
def prod_parameters_load():
    
    @task(multiple_outputs=True)
    def setup():

        setup = _setup()

        return setup
    
    @task()
    def load(task_dependency, setup, engine):

        _load(engine, **setup)

        return
    

    setup = setup()
    load(setup, setup, engine)


prod_parameters_load = prod_parameters_load()
