from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os, sys

sys.path.append('/opt/airflow/')
from dags.utils.voting.setup import _setup
from dags.utils.voting.fetch_chief import _fetch_chief
from dags.utils.voting.fetch_polls import _fetch_polls
from dags.utils.voting.create_proxies import _create_proxies
from dags.utils.voting.break_proxies import _break_proxies
from dags.utils.voting.full_proxies_history import _full_proxies_history
from dags.utils.voting.fetch_api_polls import _fetch_api_polls
from dags.utils.voting.fetch_executives import _fetch_executives
from dags.utils.voting.vote_operations import _vote_operations
from dags.utils.voting.load import _load
from dags.utils.voting.votes import _votes
from dags.utils.voting.update_current_voters_table import _update_current_voters_table
from dags.utils.voting.tests.data_validation import _data_validation
from dags.utils.voting.count_votes import _count_votes
from dags.utils.voting.voting_power_cache import _voting_power_cache


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": ['piotr.m.klis@gmail.com'],
    "email_on_failure": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}
# [END default_args]


dataset = "bigquery-public-data.crypto_ethereum"
mkr_address = "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2"
dschief1_0_date = datetime(2017, 12, 17)
dschief1_1_date = datetime(2019, 5, 6, 5, 22, 38)
dschief1_2_date = datetime(2021, 1, 1)


# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 9, 7, 10),
    max_active_runs=1,
    catchup=False,
)
def prod_votes_load():
    @task(multiple_outputs=True)
    def setup():

        environment = os.getenv('ENVIRONMENT', 'DEV')
        setup = _setup(environment)

        return setup

    @task(multiple_outputs=True)
    def fetch_chief(task_dependency, setup):

        chief = _fetch_chief(**setup)

        return {"chief": chief}

    @task(multiple_outputs=True)
    def fetch_polls(task_dependency, setup):

        polls = _fetch_polls(**setup)

        return {"polls": polls, "setup": setup}

    @task(multiple_outputs=True)
    def create_proxies(task_dependency, setup):

        up = _create_proxies(**setup)

        return {"up": up, "setup": setup}

    @task(multiple_outputs=True)
    def break_proxies(task_dependency, setup):

        down = _break_proxies(**setup)

        return {"down": down, "setup": setup}

    @task(multiple_outputs=True)
    def full_proxies_history(task_dependency, up, down, setup):

        latest_proxies_history, full_proxies_history = _full_proxies_history(up, down, **setup)

        return {
            "latest_proxies_history": latest_proxies_history,
            "full_proxies_history": full_proxies_history,
            "setup": setup,
        }

    @task(multiple_outputs=True)
    def fetch_api_polls(task_dependency, setup):

        api_polls = _fetch_api_polls(**setup)

        return {"api_polls": api_polls}

    @task(multiple_outputs=True)
    def fetch_executives(task_dependency, setup):

        execs = _fetch_executives(**setup)

        return {"execs": execs, "setup": setup}

    @task(multiple_outputs=True)
    def vote_operations(task_dependency, chief, polls, latest_proxies_history, full_proxies_history, setup):

        operations = _vote_operations(chief, polls, latest_proxies_history, full_proxies_history, **setup)

        return {"operations": operations, "setup": setup}

    @task(multiple_outputs=True)
    def votes(task_dependency, operations, polls, setup):

        votes = _votes(operations, polls, **setup)

        return {"votes": votes}

    @task()
    def load(task_dependency, chief, polls, api_polls, executives, votes, operations, setup):

        _load(chief, polls, api_polls, executives, votes, operations, **setup)

        return True

    @task()
    def update_current_voters_table(task_dependency, setup):

        _update_current_voters_table(**setup)

        return True

    @task()
    def data_validation(task_dependency, api_polls, execs, setup):

        _data_validation(api_polls, execs, **setup)

        return True

    @task()
    def count_votes(task_dependancy, setup):

        _count_votes(**setup)

        return True

    @task()
    def voting_power_cache(task_dependancy, setup):

        _voting_power_cache(**setup)

        return True

    setup = setup()
    chief = fetch_chief(setup, setup)
    polls = fetch_polls(setup, setup)

    proxies_up = create_proxies(setup, setup)
    proxies_down = break_proxies(setup, setup)
    history = full_proxies_history([proxies_up, proxies_down], proxies_up["up"], proxies_down["down"], setup)

    api_polls = fetch_api_polls(setup, setup)
    execs = fetch_executives(setup, setup)
    operations = vote_operations(
        [chief, polls, history],
        chief["chief"],
        polls["polls"],
        history["latest_proxies_history"],
        history["full_proxies_history"],
        polls["setup"],
    )

    gov_actions = votes(operations, operations["operations"], api_polls["api_polls"], operations["setup"])

    load_data = load(
        [gov_actions, api_polls, execs],
        chief["chief"],
        polls["polls"],
        api_polls["api_polls"],
        execs["execs"],
        gov_actions["votes"],
        operations["operations"],
        execs["setup"],
    )
    current_votes = update_current_voters_table(load_data, setup)
    validation = data_validation(current_votes, api_polls["api_polls"], execs["execs"], setup)
    votes_summary = count_votes(validation, setup)
    voting_power_cache(votes_summary, setup)


prod_votes_load = prod_votes_load()
