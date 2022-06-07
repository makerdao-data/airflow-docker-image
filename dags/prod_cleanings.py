import time
from datetime import datetime, timedelta, timezone
from sqlalchemy import func
import sys
sys.path.append('/opt/airflow/')

from airflow.decorators import dag, task
from airflow.utils.db import provide_session
from airflow.models import XCom


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
    schedule_interval='50 */6 * * *',
    start_date=datetime(2022, 6, 7, 9),
    max_active_runs=1,
    catchup=False,
)
def prod_clean():

    @task()
    @provide_session
    def clean_xcoms(session=None):
        
        ts_limit = datetime.now(timezone.utc) - timedelta(days=1)
        session.query(XCom).filter(XCom.execution_date <= ts_limit).delete()

        return

    clean_xcoms()

prod_clean = prod_clean()