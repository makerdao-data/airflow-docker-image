#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os, sys

sys.path.append('/opt/airflow/')
from dags.utils.liq20.updater import rounds_auctions
from dags.utils.liq20.clip import get_clipper_calls
from dags.utils.liq20.actions import get_clipper_actions
from dags.utils.liq20.dog import get_dog_calls
from dags.utils.liq20.barks import get_barks
from dags.utils.liq20.clippers import update_clippers
from dags.connectors.sf import sf
from dags.connectors.gcp import bq_query
from config import liquidations_db, STAGING

DB = liquidations_db

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'email': ['piotr.m.klis@gmail.com'],
    'email_on_failure': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
# [END default_args]

# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2021, 11, 22, 12),
    max_active_runs=1,
    catchup=False,
)
def mainnet_liquidations20():
    @task(multiple_outputs=True)
    def intro(DB=None):

        load_id = datetime.utcnow().__str__()[:19]
        proc_start = load_id

        q = f"""SELECT max(end_block), max(proc_start)
                FROM {DB}.internal.scheduler
                WHERE status = 1; """

        last_parsed_block = sf.execute(q).fetchone()
        if last_parsed_block[0] and last_parsed_block[1]:
            start_block = last_parsed_block[0] + 1
            start_time = datetime.strftime(last_parsed_block[1], '%Y-%m-%d %H:%M:%S')
        else:
            start_block = 12316360
            start_time = '2021-04-26 14:02:08'

        # prevent from running ahead of mcd.internal.prices data (needed for loading {DB}.internal.action)
        end_block, end_time = sf.execute(
            """
                SELECT MAX(block), MAX(timestamp)
                FROM mcd.staging.blocks; """
        ).fetchone()

        end_time = end_time.__str__()[:19]

        q = f"""INSERT INTO {DB}.internal.scheduler(load_id, proc_start, start_block, end_block)
                VALUES('{load_id}', '{proc_start}', {start_block}, {end_block}); """

        sf.execute(q)

        starter = dict(
            load_id=load_id,
            start_block=start_block,
            end_block=end_block,
            start_time=start_time,
            end_time=end_time,
            DB=DB,
            STAGING=STAGING,
        )

        return starter

    @task()
    def clippers(task_dependency=None, load_id=None, DB=None):
        update_clippers(load_id=load_id, DB=DB)

        return True

    @task(multiple_outputs=True)
    def dog_calls(
        task_dependency=None,
        load_id=None,
        start_block=None,
        end_block=None,
        start_time=None,
        end_time=None,
        DB=None,
        STAGING=None,
    ):
        calls = get_dog_calls(load_id, start_block, end_block, start_time, end_time, DB, STAGING)
        return calls

    @task()
    def dog_barks(
        task_dependency=None,
        load_id=None,
        start_block=None,
        end_block=None,
        start_time=None,
        end_time=None,
        DB=None,
        STAGING=None,
    ):
        get_barks(load_id, start_block, end_block, start_time, end_time, DB, STAGING)
        return True

    @task(multiple_outputs=True)
    def clipper_calls(
        task_dependency=None,
        load_id=None,
        start_block=None,
        end_block=None,
        start_time=None,
        end_time=None,
        DB=None,
        STAGING=None,
    ):
        calls = get_clipper_calls(load_id, start_block, end_block, start_time, end_time, DB, STAGING)
        return calls

    @task()
    def liquidations_actions(
        task_dependency=None,
        load_id=None,
        start_block=None,
        end_block=None,
        start_time=None,
        end_time=None,
        DB=None,
        STAGING=None,
    ):
        get_clipper_actions(load_id, start_block, end_block, start_time, end_time, DB, STAGING)
        return True

    @task()
    def create_rounds_auctions(
        task_dependency=None,
        load_id=None,
        start_block=None,
        end_block=None,
        start_time=None,
        end_time=None,
        DB=None,
        STAGING=None,
    ):
        rounds_auctions(load_id, start_block, end_block, start_time, end_time, DB, STAGING)
        return True

    @task()
    def outro(task_dependency=None, load_id=None, dog_calls=None, clipper_calls=None, DB=None):

        status = 1
        end_point = datetime.utcnow()
        proc_end = end_point.__str__()[:19]

        q = f"""UPDATE {DB}.internal.scheduler
                SET proc_end = '{proc_end}', status = {status}, dog_calls = {dog_calls}, clipper_calls = {clipper_calls}
                WHERE load_id = '{load_id}'; """

        sf.execute(q)

        return True

    starter = intro(DB=DB)
    new_clippers = clippers(task_dependency=starter, load_id=starter['load_id'], DB=starter['DB'])
    d_calls = dog_calls(
        task_dependency=new_clippers,
        load_id=starter['load_id'],
        start_block=starter['start_block'],
        end_block=starter['end_block'],
        start_time=starter['start_time'],
        end_time=starter['end_time'],
        DB=starter['DB'],
        STAGING=starter['STAGING'],
    )
    barks = dog_barks(
        task_dependency=d_calls,
        load_id=starter['load_id'],
        start_block=starter['start_block'],
        end_block=starter['end_block'],
        start_time=starter['start_time'],
        end_time=starter['end_time'],
        DB=starter['DB'],
        STAGING=starter['STAGING'],
    )
    c_calls = clipper_calls(
        task_dependency=barks,
        load_id=starter['load_id'],
        start_block=starter['start_block'],
        end_block=starter['end_block'],
        start_time=starter['start_time'],
        end_time=starter['end_time'],
        DB=starter['DB'],
        STAGING=starter['STAGING'],
    )
    actions = liquidations_actions(
        task_dependency=c_calls,
        load_id=starter['load_id'],
        start_block=starter['start_block'],
        end_block=starter['end_block'],
        start_time=starter['start_time'],
        end_time=starter['end_time'],
        DB=starter['DB'],
        STAGING=starter['STAGING'],
    )
    ra = create_rounds_auctions(
        task_dependency=actions,
        load_id=starter['load_id'],
        start_block=starter['start_block'],
        end_block=starter['end_block'],
        start_time=starter['start_time'],
        end_time=starter['end_time'],
        DB=starter['DB'],
        STAGING=starter['STAGING'],
    )
    outro = outro(
        task_dependency=ra,
        load_id=starter['load_id'],
        dog_calls=d_calls['calls'],
        clipper_calls=c_calls['calls'],
        DB=starter['DB'],
    )


# [START dag_invocation]
mainnet_liquidations20 = mainnet_liquidations20()
# [END dag_invocation]
