from datetime import datetime
import json
import os
from dags.connectors.sf import sf
from dags.connectors.gcp import bq_query
from dags.connectors.chain import chain
from config import dataset
from config import votes_db
from config import votes_scheduler
from config import staging_votes_db
from config import staging_votes_scheduler
from config import vaults_db
from config import votes_fallback_time
from config import votes_fallback_block
from config import chiefs
from config import mkr_address
from config import polls_address
from config import new_polls_address
from config import absolute_import_path


def _setup(environment):

    load_id = datetime.utcnow().__str__()[:19]

    # set proper environment (db, schema, scheduler, etc...)
    if environment == 'DEV':
        db = staging_votes_db
        scheduler = staging_votes_scheduler
    else:
        db = votes_db
        scheduler = votes_scheduler

    start_block = sf.execute(
        f"""
            SELECT MAX(end_block)
            FROM {db}.internal.votes_scheduler; """
    ).fetchone()[0]

    if start_block:

        start_time = datetime.utcfromtimestamp(chain.eth.getBlock(start_block)['timestamp']).__str__()[:19]

    else:

        start_block = votes_fallback_block
        start_time = votes_fallback_time

    if os.getenv('MAX_BLOCK') and os.getenv('MAX_TIMESTAMP'):

        end_block = int(os.getenv('MAX_BLOCK'))
        end_time = os.getenv('MAX_TIMESTAMP')

    else:

        # get last safe block and safe datetime to pull data from BQ
        q = """SELECT max(number) - 10, max(timestamp)
            FROM `bigquery-public-data.crypto_ethereum.blocks`; """

        last_block = bq_query(q)
        end_block = last_block[0][0]
        end_time = last_block[0][1]

    with open(absolute_import_path + 'chief.json', 'r') as fp:
        chief_abi = json.load(fp)

    chief_addresses = list(chiefs.keys())

    with open(absolute_import_path + 'polls.json', 'r') as fp:
        polls_abi = json.load(fp)

    # new polls
    with open(absolute_import_path + 'polls_new.json', 'r') as fp:
        new_polls_abi = json.load(fp)

    create_proxy_topic = '0xf001c2d12c2288935c811b4977748cb3e5e3c485d08a1fb1984023cb2452d463'.lower()
    vote_proxy_factory = '0x6fcd258af181b3221073a96dd90d1f7ae7eec408'.lower()
    old_vote_proxy_factory = '0x868ba9aeacA5B73c7C27F3B01588bf4F1339F2bC'.lower()

    output = dict(
        dataset=dataset,
        mkr_address=mkr_address,
        dschief1_0_date=datetime(2017, 12, 17).__str__(),
        dschief1_1_date=datetime(2019, 5, 6, 5, 22, 38).__str__(),
        dschief1_2_date=datetime(2021, 1, 1).__str__(),
        load_id=load_id,
        start_block=start_block,
        start_time=start_time.__str__()[:19],
        end_block=end_block,
        end_time=end_time.__str__()[:19],
        chief_addresses=chief_addresses,
        chief_abi=chief_abi,
        polls_address=polls_address,
        polls_abi=polls_abi,
        new_polls_address=new_polls_address,
        new_polls_abi=new_polls_abi,
        votes_db=db,
        votes_scheduler=scheduler,
        vaults_db=vaults_db,
        create_proxy_topic=create_proxy_topic,
        vote_proxy_factory=vote_proxy_factory,
        old_vote_proxy_factory=old_vote_proxy_factory,
    )

    return output
