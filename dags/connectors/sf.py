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

import snowflake.connector
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
from random import randint
from airflow.exceptions import AirflowFailException
from random import randint
import os, sys
import json

sys.path.append('/opt/airflow/')
from config import SNOWFLAKE_CONNECTION

# Snowflake Connector
connection = snowflake.connector.connect(**SNOWFLAKE_CONNECTION)
sf = connection.cursor()
sf_dict = connection.cursor(snowflake.connector.DictCursor)

# SqlAlchemy Connector
sa_connection = create_engine(URL(**SNOWFLAKE_CONNECTION))
sa = sa_connection.connect()

def clear_stage(stage):
    sf.execute("REMOVE @%s" % stage)


def write_to_stage(records, stage):

    success = False
    if records:
        csv_file = open('dump.csv', 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join(
                [attribute.__str__().replace('|', '')
                 for attribute in record]))
        csv_file.close()

        try:
            sf.execute("PUT file://dump.csv @%s" % stage)
            success = True
            if os.path.exists('dump.csv'):
                os.remove('dump.csv')
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate,
                                                      e.msg, e.sfqid))
            if os.path.exists('dump.csv'):
                os.remove('dump.csv')
            raise AirflowFailException("#ERROR ON LOADING DATA")

    return success


def write_to_table(stage, table, purge=True):

    success = False
    try:
        sf.execute(
            "COPY INTO %s FROM @%s FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PURGE=%s; "
            % (table, stage, purge))
        success = True
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg,
                                                  e.sfqid))
        raise AirflowFailException("#ERROR ON LOADING DATA")

    return success


def sf_disconnect(cursor):
    cursor.connection.close()


def transaction_clear_stage(conn, stage):
    conn.execute("REMOVE @%s" % stage)


def transaction_write_to_stage(conn, records, stage):

    if records:
        csv_file = open('dump.csv', 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join(
                [attribute.__str__().replace('|', '')
                 for attribute in record]))
        csv_file.close()

        conn.execute("PUT file://dump.csv @%s" % stage)

        if os.path.exists('dump.csv'):
            os.remove('dump.csv')

    return True


def transaction_write_to_table(conn, stage, table, purge=True):

    conn.execute(
        "COPY INTO %s FROM @%s FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PURGE=%s; "
        % (table, stage, purge))

    return True


def updated_write_to_stage(records, stage):

    success = False
    if records:
        csv_file = open('dump.csv', 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join(
                [attribute.__str__().replace('|', '')
                 for attribute in record]))
        csv_file.close()

        sf.execute("PUT file://dump.csv @%s" % stage)
        success = True

        if os.path.exists('dump.csv'):
            os.remove('dump.csv')

    return success


def updated_write_to_table(stage, table, purge=True):

    success = False
    sf.execute(
        "COPY INTO %s FROM @%s FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PURGE=%s; "
        % (table, stage, purge))
    success = True

    return success


def _write_barks_to_table(conn, stage, table, pattern, purge=True):

    success = False
    try:
        conn.execute(
            f"""COPY INTO {table}(LOAD_ID, BLOCK, TIMESTAMP, TX_HASH, URN, VAULT,
                    ILK, OWNER, COLLATERAL, DEBT, PENALTY, RATIO, OSM_PRICE, MKT_PRICE,
                    AUCTION_ID, CALLER, KEEPER, GAS_USED, STATUS, REVERT_REASON
                    ) FROM @{stage}/{pattern} FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PURGE={purge}; """
        )
        success = True

    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg,
                                                  e.sfqid))
        raise AirflowFailException("#ERROR ON LOADING DATA")

    return success


def custom_write_to_stage(records, stage):

    success = False
    if records:

        file_name = 'dump_' + str(randint(1, 999999999)).zfill(9) + '.csv'

        csv_file = open(file_name, 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join(
                [attribute.__str__().replace('|', '')
                 for attribute in record]))
        csv_file.close()

        try:
            sf.execute("PUT file://%s @%s" % (file_name, stage))
            success = True
            if os.path.exists(file_name):
                os.remove(file_name)
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate,
                                                      e.msg, e.sfqid))
            if os.path.exists(file_name):
                os.remove(file_name)
            raise AirflowFailException("#ERROR ON LOADING DATA")

    return success


def _write_to_stage(conn, records, stage):

    file_name = None
    if records:

        file_name = 'dump_' + str(randint(1, 999999999)).zfill(9) + '.csv'

        csv_file = open(file_name, 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join(
                [attribute.__str__().replace('|', '')
                 for attribute in record]))
        csv_file.close()

        try:
            conn.execute("PUT file://%s @%s" % (file_name, stage))
            if os.path.exists(file_name):
                os.remove(file_name)
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate,
                                                      e.msg, e.sfqid))
            if os.path.exists(file_name):
                os.remove(file_name)
            raise AirflowFailException("#ERROR ON LOADING DATA")

    return file_name


def _clear_stage(conn, stage, pattern):
    conn.execute(f"""REMOVE @{stage}/{pattern}""")


def _write_to_table(conn, stage, table, pattern, purge=True):

    conn.execute(
        f"""COPY INTO {table} FROM @{stage}/{pattern}.gz FILE_FORMAT=(TYPE=CSV, FIELD_DELIMITER='|', NULL_IF='None') PURGE={purge}; """
    )

    return True


def _write_actions_to_table(conn, stage, table, pattern, purge=True):

    success = False
    try:
        conn.execute(f"""COPY INTO {table}(
                LOAD_ID, AUCTION_ID, TIMESTAMP, BLOCK, TX_HASH,
                TYPE, CALLER, DATA, DEBT, INIT_PRICE, MAX_COLLATERAL_AMT,
                AVAILABLE_COLLATERAL, SOLD_COLLATERAL, MAX_PRICE,
                COLLATERAL_PRICE, OSM_PRICE, RECOVERED_DEBT, CLOSING_TAKE,
                KEEPER, INCENTIVES, URN, GAS_USED, STATUS, REVERT_REASON,
                ROUND, BREADCRUMB, ILK, MKT_PRICE
                ) FROM @{stage}/{pattern}.gz FILE_FORMAT=(TYPE=CSV,FIELD_DELIMITER = '|',NULL_IF='None') PURGE={purge}; """
                     )
        success = True
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg,
                                                  e.sfqid))
        raise AirflowFailException("#ERROR ON LOADING DATA")

    return success


def _write_json_to_stage(conn, data_object, stage):

    file_name = None
    if data_object:

        file_name = 'dump_' + str(randint(1, 999999999)).zfill(9) + '.json'

        with open(file_name, "w") as outfile:
            json.dump(data_object, outfile)

        try:
            conn.execute(f"""PUT file://{file_name} @{stage}""")
            if os.path.exists(file_name):
                os.remove(file_name)
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate,
                                                      e.msg, e.sfqid))
            if os.path.exists(file_name):
                os.remove(file_name)

    return file_name
