from random import randint
import os
import snowflake.connector
from airflow.exceptions import AirflowFailException


def transaction_write_to_stage(conn, records, stage):

    file_name = None
    if records:

        file_name = 'dump_' + str(randint(1, 999999999)).zfill(9) + '.csv'

        csv_file = open(file_name, 'w')
        for record in records:
            csv_file.write('%s\n' % '|'.join([attribute.__str__().replace('|', '') for attribute in record]))
        csv_file.close()

        try:
            conn.execute("PUT file://%s @%s" % (file_name, stage))
            if os.path.exists(file_name):
                os.remove(file_name)
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            if os.path.exists(file_name):
                os.remove(file_name)
            raise AirflowFailException("#ERROR ON LOADING DATA")

    return file_name


def transaction_clear_stage(conn, stage, pattern):
    conn.execute(f"""REMOVE @{stage}/{pattern}""")
