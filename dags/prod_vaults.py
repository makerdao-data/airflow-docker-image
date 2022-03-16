from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.utils.vaults.setup import _setup
from dags.utils.vaults.cohesion_check import _cohesion_check
from dags.utils.vaults.current_vaults import _current_vaults
from dags.utils.vaults.fetch_blocks import _fetch_blocks
from dags.utils.vaults.fetch_external_prices import _fetch_external_prices
from dags.utils.vaults.fetch_ilks import _fetch_ilks
from dags.utils.vaults.fetch_manager import _fetch_manager
from dags.utils.vaults.fetch_oracles import _fetch_oracles
from dags.utils.vaults.fetch_prices import _fetch_prices
from dags.utils.vaults.fetch_rates import _fetch_rates
from dags.utils.vaults.fetch_ratios import _fetch_ratios
from dags.utils.vaults.fetch_vat import _fetch_vat
from dags.utils.vaults.load import _load
from dags.utils.vaults.manager_operations import _manager_operations
from dags.utils.vaults.opened_vaults import _opened_vaults
from dags.utils.vaults.post_load_check import _post_load_check
from dags.utils.vaults.pre_load_check import _pre_load_check
from dags.utils.vaults.setup import _setup
from dags.utils.vaults.vat_operations import _vat_operations
from dags.utils.vaults.vault_operations import _vault_operations
from dags.utils.vaults.vaults import _vaults
from dags.utils.vaults.debt_calculation import _debt_calculation
from dags.trackers.vault_updater import update_vault_data

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

dataset = "bigquery-public-data.crypto_ethereum"


# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2021, 11, 22, 12),
    max_active_runs=1,
    catchup=False,
)
def prod_vaults_load():

    @task(multiple_outputs=True)
    def setup():

        environment = os.getenv('ENVIRONMENT', 'DEV')
        setup = _setup(environment)

        return setup

    @task(multiple_outputs=True)
    def fetch_blocks(task_dependency, setup):

        blocks = _fetch_blocks(**setup)

        return {"blocks": blocks}

    @task(multiple_outputs=True)
    def fetch_vat(task_dependency, setup):

        vat = _fetch_vat(**setup)

        return {"vat": vat}

    @task(multiple_outputs=True)
    def vat_operations(task_dependency, vat, setup):

        vat_operations = _vat_operations(vat, **setup)

        return {"vat_operations": vat_operations}

    @task(multiple_outputs=True)
    def fetch_manager(task_dependency, setup):

        manager = _fetch_manager(**setup)

        return {"manager": manager}

    @task(multiple_outputs=True)
    def manager_operations(task_dependency, manager, setup):

        manager_operations = _manager_operations(manager, **setup)

        return {"manager_operations": manager_operations}

    @task(multiple_outputs=True)
    def opened_vaults(task_dependency, manager, setup):

        opened_vaults = _opened_vaults(manager, **setup)

        return {"opened_vaults": opened_vaults}

    @task(multiple_outputs=True)
    def vault_operations(task_dependency, vat_operations, manager_operations,
                         opened_vaults, setup):

        vault_operations = _vault_operations(vat_operations,
                                             manager_operations, opened_vaults,
                                             **setup)

        return {"vault_operations": vault_operations}

    @task()
    def fetch_ilks(task_dependency, vat, setup):

        _fetch_ilks(vat, **setup)

        return

    @task(multiple_outputs=True)
    def fetch_oracles(task_dependency, setup):

        oracles = _fetch_oracles(**setup)

        return {'oracles': oracles}

    @task(multiple_outputs=True)
    def fetch_ratios(task_dependency, setup):

        ratios = _fetch_ratios(**setup)

        return {'ratios': ratios}

    @task(multiple_outputs=True)
    def fetch_rates(task_dependency, blocks, vat, setup):

        rates = _fetch_rates(blocks, vat, **setup)

        return {'rates': rates}

    @task(multiple_outputs=True)
    def fetch_external_prices(setup):

        external_prices = _fetch_external_prices(**setup)

        return {'external_prices': external_prices}

    @task(multiple_outputs=True)
    def fetch_prices(task_dependency, blocks, oracles, external_prices, setup):

        prices = _fetch_prices(blocks, oracles, external_prices, **setup)

        return {'prices': prices}

    @task(multiple_outputs=True)
    def cohesion_check(task_dependency, blocks, vat, manager, rates, prices,
                       setup):

        test = _cohesion_check(blocks, vat, manager, rates, prices, **setup)

        return {'test': test}

    @task()
    def pre_load_check(task_dependency, blocks, vat, manager, setup):

        _pre_load_check(blocks, vat, manager, **setup)

        return

    @task(multiple_outputs=True)
    def vaults(task_dependency, ratios, vault_operations, rates, prices,
               setup):

        public_vaults = _vaults(ratios, vault_operations, rates, prices,
                                **setup)

        return {'public_vaults': public_vaults}

    @task()
    def load(task_dependency, blocks, vat, vat_operations, manager,
             manager_operations, ratios, rates, prices, vaults_operations,
             public_vaults, setup):

        _load(blocks, vat, vat_operations, manager, manager_operations, ratios,
              rates, prices, vaults_operations, public_vaults, **setup)

        return

    @task()
    def current_vaults(task_dependency, setup):

        _current_vaults(**setup)

        return

    @task()
    def post_load_check(task_dependency, setup):

        _post_load_check(**setup)

        return

    @task()
    def debt_check(task_dependency, setup):

        _debt_calculation(**setup)

        return

    @task()
    def update_vault_tracker(task_dependency):

        update_vault_data(sf)

        return

    setup = setup()
    blocks = fetch_blocks(setup, setup)
    vat = fetch_vat(setup, setup)
    vat_ops = vat_operations(vat, vat['vat'], setup)
    manager = fetch_manager(setup, setup)
    manager_ops = manager_operations(manager, manager['manager'], setup)
    all_vaults = opened_vaults(manager, manager['manager'], setup)
    vault_ops = vault_operations(
        [vat_ops, manager_ops, all_vaults],
        vat_ops['vat_operations'],
        manager_ops['manager_operations'],
        all_vaults['opened_vaults'],
        setup,
    )
    ilks = fetch_ilks(vat, vat['vat'], setup)
    oracles = fetch_oracles(ilks, setup)
    ratios = fetch_ratios(setup, setup)
    rates = fetch_rates([blocks, vat], blocks['blocks'], vat['vat'], setup)
    external_prices = fetch_external_prices(setup)
    prices = fetch_prices(
        [blocks, oracles, external_prices],
        blocks['blocks'],
        oracles['oracles'],
        external_prices['external_prices'],
        setup,
    )
    cohesion = cohesion_check(
        [blocks, vat, manager, rates],
        blocks['blocks'],
        vat['vat'],
        manager['manager'],
        rates['rates'],
        prices['prices'],
        setup,
    )
    pre = pre_load_check([blocks, vat, manager], blocks['blocks'], vat['vat'],
                         manager['manager'], setup)
    public = vaults(cohesion, ratios['ratios'], vault_ops['vault_operations'],
                    rates['rates'], prices['prices'], setup)
    upload = load(
        [blocks, vat, manager, ratios, rates, prices, vat_ops, cohesion, pre],
        blocks['blocks'],
        vat['vat'],
        vat_ops['vat_operations'],
        manager['manager'],
        manager_ops['manager_operations'],
        ratios['ratios'],
        rates['rates'],
        prices['prices'],
        vault_ops['vault_operations'],
        public['public_vaults'],
        setup,
    )
    curr_vaults = current_vaults(upload, setup)
    post = post_load_check(curr_vaults, setup)
    debt = debt_check(curr_vaults, setup)
    update_vault_tracker(debt)


prod_vaults_load = prod_vaults_load()
