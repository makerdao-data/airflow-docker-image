from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os, sys

sys.path.append('/opt/airflow/')

from dags.connectors.sf import sf
from dags.utils.vaults_at_risk.setup import _setup
from dags.utils.vaults_at_risk.urns import _fetch_urns
from dags.utils.vaults_at_risk.rates import _fetch_rates
from dags.utils.vaults_at_risk.mats import _fetch_mats
from dags.utils.vaults_at_risk.pips import _fetch_pips
from dags.utils.vaults_at_risk.prices import _fetch_prices
from dags.utils.vaults_at_risk.proc import _proc


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
    schedule_interval='0 10 * * *',
    start_date=datetime(2022, 4, 5, 10),
    max_active_runs=1,
    catchup=False,
)

def prod_vaults_at_risk():

    @task(multiple_outputs=True)
    def setup():
        start_block, last_scanned_block, latest_timestamp = _setup()
        return dict(
            start_block=start_block,
            last_scanned_block=last_scanned_block,
            latest_timestamp=latest_timestamp
        )

    @task()
    def urns(task_dependency, setup):
        _fetch_urns(sf, setup['start_block'], setup['last_scanned_block'])
        return

    @task()
    def rates(task_dependency, setup):
        _fetch_rates(sf, setup['start_block'], setup['last_scanned_block'])
        return
    
    @task()
    def mats(task_dependency, setup):
        _fetch_mats(sf, setup['start_block'], setup['last_scanned_block'])
        return
    
    @task()
    def pips(task_dependency, setup):
        _fetch_pips(sf, setup['start_block'], setup['last_scanned_block'])
        return
    
    @task()
    def prices(task_dependency, setup):
        _fetch_prices(sf, setup['start_block'], setup['last_scanned_block'])
        return
    
    @task()
    def proc(task_dependency, setup):
        _proc(sf, setup['last_scanned_block'], setup['latest_timestamp'])

    @task()
    def outro(task_dependency, setup):
        sf.execute(f"""
            UPDATE MAKER.RISK.VAULTS
            SET last_scanned_block = {setup['last_scanned_block']}
            WHERE ID = 1;
        """)
        return
    
    setup = setup()
    u = urns(setup, setup)
    r = rates(setup, setup)
    m = mats(setup, setup)
    pips = pips(setup, setup)
    prices = prices(pips, setup)
    proc([setup, u, r, m, pips, prices], setup)
    outro(proc, setup)

prod_vaults_at_risk = prod_vaults_at_risk()