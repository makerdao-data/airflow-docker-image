import sys
from datetime import datetime, timedelta
import snowflake.connector

sys.path.append('/opt/airflow/')

from airflow.decorators import dag, task
from dags.utils.starknet.dai_bridge import starknet_dai_bridge_events
from dags.utils.starknet.dai_escrow import starknet_dai_escrow_events
from dags.utils.starknet.l2_dai_transfers import l2_dai_transfers

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
    schedule_interval=None, #'*/30 * * * *',
    start_date=datetime(2022, 7, 11, 12),
    max_active_runs=1,
    catchup=False,
)
def prod_starknet():

    @task()
    def dai_bridge():

        starknet_dai_bridge_events()

        return

    @task()
    def dai_escrow():

        starknet_dai_escrow_events()

        return
    
    @task()
    def dai_transfers():

        l2_dai_transfers()

        return

    dai_bridge()
    dai_escrow()
    dai_transfers()


prod_starknet = prod_starknet()
