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
from connectors.sf import sf, write_to_stage, write_to_table, clear_stage

DB_SCHEMA_TABLE = 'tokens.staging.dai_balances'


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'email': ['piotr.m.klis@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
# [END default_args]


# get current balances
def get_current_dai_balances(date):

    query = """
        SELECT address, balance
        FROM {}
        WHERE date = '{}'
        """.format(
        DB_SCHEMA_TABLE, date
    )

    sf.execute(query)
    records = sf.fetchall()

    balances = dict()
    if records != []:
        for i in records:
            balances[i[0]] = i[1]

    return balances


# [START tools]
def get_dai_transfers(date):

    query = """
        SELECT timestamp, from_address, to_address, amount
        FROM TOKENS.STAGING.TRANSFERS
        WHERE
            token = '0x6b175474e89094c44da98b954eedeac495271d0f'
            AND DATE(timestamp) = '{}'
        ORDER BY timestamp
        """.format(
        date
    )

    sf.execute(query)
    records = sf.fetchall()

    return records


def update_balances(balances, transfers):

    for i in transfers:

        if i[3] > 0:

            # from_
            if i[1] not in balances:
                if i[1] != '0x0000000000000000000000000000000000000000':
                    balances[i[1]] = i[3] * -1

            else:

                current_address_balance = balances[i[1]]
                updated_address_balance = current_address_balance + i[3] * -1
                # remove 0 balance address
                if updated_address_balance == 0:
                    del balances[i[1]]
                else:
                    balances[i[1]] = updated_address_balance

            # to_
            if i[2] != '0x0000000000000000000000000000000000000000':
                if i[2] not in balances:
                    balances[i[2]] = i[3]

                else:

                    current_address_balance = balances[i[2]]
                    updated_address_balance = current_address_balance + i[3]
                    # remove 0 balance address
                    if updated_address_balance == 0:
                        del balances[i[2]]
                    else:
                        balances[i[2]] = updated_address_balance

    return balances


def save_balances(balances, date):

    load_id = datetime.now().__str__()[:19]
    records = []
    for i in balances.items():
        records.append([load_id, date, i[0], i[1]])

    success = False
    try:
        sf.execute('USE TOKENS;')
        sf.execute('USE SCHEMA STAGING;')
        clear_stage('DAI_BALANCES_SPACE')
        write_to_stage(records, 'DAI_BALANCES_SPACE')
        write_to_table('DAI_BALANCES_SPACE', DB_SCHEMA_TABLE)
        success = True
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    return success


def quality_test(date):

    query_from = """select sum(amount)
            from tokens.staging.transfers
            where
                token = '0x6b175474e89094c44da98b954eedeac495271d0f'
                and DATE(timestamp) <= '{}'
                and from_address = '0x0000000000000000000000000000000000000000';""".format(
        date
    )

    query_to = """select sum(amount)
                from tokens.staging.transfers
                where
                    token = '0x6b175474e89094c44da98b954eedeac495271d0f'
                    and DATE(timestamp) <= '{}'
                    and to_address = '0x0000000000000000000000000000000000000000';""".format(
        date
    )

    balance_for_date = """select sum(balance)
                        from {}
                        where date = '{}';""".format(
        DB_SCHEMA_TABLE, date
    )

    sf.execute(query_from)
    minted = sf.fetchone()
    sf.execute(query_to)
    burned = sf.fetchone()
    sf.execute(balance_for_date)
    balance = sf.fetchone()

    if minted[0] and burned[0] and balance[0]:
        print('Minted DAI: {};'.format(minted[0] / 10 ** 18))
        print('Burned DAI: {};'.format(burned[0] / 10 ** 18))
        print('DAI total balance for {}: {};'.format(date, balance[0] / 10 ** 18))

        if minted[0] - burned[0] == balance[0]:
            print('Test result: passed')
            return True
        else:
            print('Test result: failed')
            return False

    return True


# [END tools]

# [START instantiate_dag]
@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 31, 1),
    max_active_runs=1,
    catchup=False,
)
def dai_balances_load():
    @task()
    def processing():
        # the day before yesterday
        query_the_day_before_yesterday = """select max(date)
                                            from {};""".format(
            DB_SCHEMA_TABLE
        )
        sf.execute(query_the_day_before_yesterday)
        the_day_before_yesterday = sf.fetchone()[0]

        if the_day_before_yesterday:
            yesterday = the_day_before_yesterday + timedelta(days=1)
            if yesterday < datetime.utcnow().date():

                print('The day before yesterday: {}'.format(the_day_before_yesterday))
                print('Yesterday: {}'.format(yesterday))

                # get the balances from the day before yestarday
                balances = get_current_dai_balances(the_day_before_yesterday)
                # get DAI transfers from yesterday
                transfers = get_dai_transfers(yesterday)
                # update balances with DAI transfers from yesterday
                balances = update_balances(balances, transfers)
                # save updated balances to db
                save = save_balances(balances, yesterday)
                # run quality check
                quality_test(yesterday)

            else:
                print("Nothing to do...")

        return yesterday.strftime('%Y-%m-%d %H:%M-%S')

    # [START main_flow]
    processing()
    # [END main_flow]


# [START dag_invocation]
dai_balances_load = dai_balances_load()
# [END dag_invocation]
