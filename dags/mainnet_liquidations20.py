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
from dags.utils.liq20.actions import clips_into_db
from dags.utils.liq20.dog import get_dog_calls
from dags.utils.liq20.barks import barks_into_db
from dags.utils.liq20.clippers import update_clippers
from dags.utils.liq20.test import liquidations_test
from dags.connectors.sf import sf
from dags.connectors.chain import chain
from dags.connectors.gcp import bq_query


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'email': ['piotr.m.klis@gmail.com', 'airflow@data.makerdao.network'],
    'email_on_failure': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
# [END default_args]

# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 11, 22, 12),
    max_active_runs=1,
    catchup=False,
)
def mainnet_liquidations20():

    @task(multiple_outputs=True)
    def intro(DB=None):

        load_id = datetime.utcnow().__str__()[:19]
        proc_start = load_id

        q = f"""
            SELECT max(end_block), max(proc_start)
            FROM {DB}.internal.scheduler
            WHERE status = 1;
        """

        last_parsed_block = sf.execute(q).fetchone()

        if last_parsed_block[0] and last_parsed_block[1]:
            start_block = last_parsed_block[0] + 1
            block_timestamp = chain.eth.get_block(start_block)['timestamp']
            start_time = datetime.utcfromtimestamp(block_timestamp).__str__()[:19]
        else:
            start_block = 12246413
            start_time = '2021-04-15 00:00:00'

        # prevent from running ahead of mcd.internal.prices data
        # needed for loading liquidations.internal.action
        end_block, end_time = sf.execute(f"""
            SELECT MAX(block), MAX(timestamp)
            FROM mcd.staging.blocks;
        """).fetchone()

        end_time = end_time.__str__()[:19]

        q = f"""
            INSERT INTO {DB}.internal.scheduler(load_id, proc_start, start_block, end_block)
            VALUES('{load_id}', '{proc_start}', {start_block}, {end_block});
        """

        sf.execute(q)

        setup = dict(
            load_id=load_id,
            start_block=start_block,
            end_block=end_block,
            start_time=start_time,
            end_time=end_time,
            DB=f'{DB}',
            STAGING=f'{DB}.STAGING.LIQUIDATIONS_EXTRACTS',
        )

        print(setup)

        return setup


    @task()
    def clippers(task_dependency, setup):

        update_clippers(**setup)

        return


    @task(multiple_outputs=True)
    def dog_calls(task_dependency, setup):

        dc = get_dog_calls(**setup)
        
        return dc


    @task()
    def dog_barks(task_dependency, setup):

        barks_into_db(**setup)

        return


    @task(multiple_outputs=True)
    def clipper_calls(task_dependency, setup):

        cc = get_clipper_calls(**setup)

        return cc


    @task()
    def liquidations_actions(task_dependency, setup):

        clips_into_db(**setup)

        return


    @task()
    def create_rounds_auctions(task_dependency, setup):
        
        rounds_auctions(**setup)

        return


    @task()
    def outro(task_dependency, load_id, dog_calls, clipper_calls, DB):

        status = 1
        end_point = datetime.utcnow()
        proc_end = end_point.__str__()[:19]

        q = f"""UPDATE {DB}.internal.scheduler
                SET proc_end = '{proc_end}', status = {status}, dog_calls = {dog_calls}, clipper_calls = {clipper_calls}
                WHERE load_id = '{load_id}'; """

        sf.execute(q)

        return True
    
    @task()
    def test(task_dependency, setup):

        liquidations_test(**setup)

        return


    setup = intro(DB='FINAL_TEST_LIQUIDATIONS')

    new_clippers = clippers(setup, setup)
    d_calls = dog_calls(new_clippers, setup)
    barks = dog_barks(d_calls, setup)
    c_calls = clipper_calls(barks, setup)
    actions = liquidations_actions(c_calls, setup)
    ra = create_rounds_auctions(actions, setup)
    o = outro(
        ra,
        load_id=setup['load_id'],
        dog_calls=d_calls['calls'],
        clipper_calls=c_calls['calls'],
        DB=setup['DB'],
    )
    t = test(o, setup)


# [START dag_invocation]
mainnet_liquidations20 = mainnet_liquidations20()
# [END dag_invocation]
