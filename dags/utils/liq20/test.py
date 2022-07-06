from airflow.exceptions import AirflowFailException
import os, sys
from web3 import Web3

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.chain import chain


def liquidations_test(**setup):

    # testing if we have all Clippers in our dataset
    # by comparing the set of ilks from Bark events
    # with the set of Clippers from our table
    test = sf.execute(f"""
        select count(distinct ilk)
        from {setup['DB']}.internal.bark
        where ilk not in (select ilk from {setup['DB']}.internal.clipper)
        and status = 1;
    """).fetchone()[0]

    if not test == 0:
        raise AirflowFailException("#ERROR: some Clppers are missing")

    # checking if every bark is followed by auction kick off
    test1 = sf.execute(f"""
        select (select count(*)
        from {setup['DB']}.internal.bark
        where status = 1) = (select count(*)
            from {setup['DB']}.internal.action
            where type = 'kick'
            and status = 1);
    """).fetchone()[0]

    if not test1:
        raise AirflowFailException("#ERROR: number of Barks and kicked off auctions does not match")


    # chcecking if our total number of kicked off auctions is equal to the one stored in Clippers contracts
    clippers = sf.execute(f"""
        select clip, ilk
        from {setup['DB']}.internal.clipper;
    """).fetchall()

    ABI = """[{"inputs":[],"name":"kicks","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]"""

    for clip, ilk in clippers:

        c = chain.eth.contract(address=Web3.toChecksumAddress(clip), abi=ABI)
        chain_kicks = 0
        try:
            chain_kicks = c.functions.kicks().call(block_identifier=setup['end_block'])
        except:
            pass

        dicu_kicks = sf.execute(f"""
            select count(*)
            from {setup['DB']}.internal.action
            where type = 'kick'
            and ilk = '{ilk}'
            and status = 1;
        """).fetchone()[0]

        if chain_kicks != dicu_kicks:

            raise AirflowFailException(f"""#ERROR: kicked off auctions: {chain_kicks}; dicu kicked off acutions: {dicu_kicks}; ilk: {ilk}""")