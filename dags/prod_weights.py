from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.utils.weights.setup import _setup
from dags.utils.weights.fetch_vote_delegates import _fetch_vote_delegates
from dags.utils.weights.get_eod_deposit import _get_eod_deposit
from dags.utils.weights.gsheet_push import _gsheet_push
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage
from dags.utils.weights.update_delegates import _update_delegates
from dags.utils.weights.gsheet_push import _gsheet_push


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
    schedule_interval='0 3 * * *',
    start_date=datetime(2021, 11, 22, 12),
    max_active_runs=1,
    catchup=False,
)
def prod_weights_load():

    @task()
    def workflow():

        days = _setup()

        if days:

            print()
            print(f'start: {days[0].__str__()[:10]}')
            print(f'end: {days[-1].__str__()[:10]}')
            print()

            load_id = datetime.now().__str__()[:19]

            _fetch_vote_delegates(days[0].__str__()[:10], days[-1].__str__()[:10], load_id)
            _update_delegates()

            delegates = sf.execute("""
                SELECT timestamp, vote_delegate
                FROM delegates.public.delegates;
            """
            ).fetchall()

            records = list()
            for d in days:
                for timestamp, vote_delegate in delegates:

                    # get deposit for given d only if vote delegate existed
                    if d >= timestamp.date():

                        b = _get_eod_deposit(d, vote_delegate)

                        r = [
                            load_id,
                            d.__str__()[:10],
                            vote_delegate,
                            b
                        ]

                        records.append(r)
            
            if records:
                pattern = _write_to_stage(
                    sf, records, "delegates.public.extracts"
                )
                if pattern:
                    _write_to_table(
                        sf,
                        "delegates.public.extracts",
                        "delegates.public.power",
                        pattern,
                    )
                    _clear_stage(sf, "delegates.public.extracts", pattern)
                
                _gsheet_push()

            sf.execute(f"""
                INSERT INTO delegates.public.scheduler(load_id, start_date, end_date)
                VALUES('{load_id}', '{days[0].__str__()[:10]}', '{days[-1].__str__()[:10]}');
            """
            )

    workflow()
    

prod_weights_load = prod_weights_load()
