import json
from datetime import datetime
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.gcp import bq_query
from dags.connectors.sf import sf
from config import absolute_import_path
from config import vaults_db
from config import vaults_scheduler
from config import staging_vaults_db
from config import staging_vaults_scheduler
from airflow.exceptions import AirflowFailException


def _setup(environment):

    # set proper environment (db, schema, scheduler, etc...)
    if environment == 'DEV':
        db = staging_vaults_db
        scheduler = staging_vaults_scheduler
    else:
        db = vaults_db
        scheduler = vaults_scheduler

    fallback_block = 8928151
    fallback_time = '2019-11-13 00:00:00'
    load_id = datetime.now().__str__()[:19]

    vat_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
    with open(absolute_import_path + 'vat.json', 'r') as fp:
        vat_abi = json.load(fp)

    manager_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
    with open(absolute_import_path + 'cdpmanager.json', 'r') as fp:
        manager_abi = json.load(fp)

    dataset = "bigquery-public-data.crypto_ethereum"

    start_block, start_time = sf.execute(
        f"""
            SELECT MAX(block), MAX(timestamp)
            FROM {db}.staging.blocks; """
    ).fetchone()

    if not start_block:
        start_block = fallback_block
        start_time = fallback_time

    # for tests
    if os.getenv('MAX_BLOCK', None) and os.getenv('MAX_TIMESTAMP', None):

        end_block = int(os.getenv('MAX_BLOCK'))
        end_time = os.getenv('MAX_TIMESTAMP')

    else:

        # get last safe block and safe datetime to pull data from BQ
        q = """SELECT max(number) - 15, max(timestamp)
            FROM `bigquery-public-data.crypto_ethereum.blocks`;
        """

        last_block = bq_query(q)
        end_block = last_block[0][0]
        end_time = last_block[0][1]

    setup = {
        'load_id': load_id,
        'vat_address': vat_address,
        'vat_abi': vat_abi,
        'manager_address': manager_address,
        'manager_abi': manager_abi,
        'dataset': dataset,
        'start_block': start_block,
        'start_time': start_time.__str__()[:19],
        'end_block': end_block,
        'end_time': end_time.__str__()[:19],
        'db': db,
        'scheduler': scheduler,
    }

    return setup
