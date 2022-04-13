from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import os, sys
import json
from web3 import Web3

sys.path.append('/opt/airflow/')

from dags.connectors.sf import sf
from dags.connectors.chain import chain


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
    schedule_interval='35 * * * *',
    start_date=datetime(2022, 4, 13, 10),
    max_active_runs=1,
    catchup=False,
)

def prod_vaults_at_risk_qa():

    @task()
    def qa():

        var_obj = sf.execute("""
            select urn, last_scanned_block
            from maker.risk.vaults
            where id = 1;
        """).fetchone()

        u = json.loads(var_obj[0])

        vat_address = "0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B"
        vat_abi = """[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":true,"inputs":[{"indexed":true,"internalType":"bytes4","name":"sig","type":"bytes4"},{"indexed":true,"internalType":"bytes32","name":"arg1","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg2","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg3","type":"bytes32"},{"indexed":false,"internalType":"bytes","name":"data","type":"bytes"}],"name":"LogNote","type":"event"},{"constant":true,"inputs":[],"name":"Line","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cage","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"can","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"dai","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"debt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"deny","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"bytes32","name":"what","type":"bytes32"},{"internalType":"uint256","name":"data","type":"uint256"}],"name":"file","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"what","type":"bytes32"},{"internalType":"uint256","name":"data","type":"uint256"}],"name":"file","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"flux","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"i","type":"bytes32"},{"internalType":"address","name":"u","type":"address"},{"internalType":"int256","name":"rate","type":"int256"}],"name":"fold","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"int256","name":"dink","type":"int256"},{"internalType":"int256","name":"dart","type":"int256"}],"name":"fork","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"i","type":"bytes32"},{"internalType":"address","name":"u","type":"address"},{"internalType":"address","name":"v","type":"address"},{"internalType":"address","name":"w","type":"address"},{"internalType":"int256","name":"dink","type":"int256"},{"internalType":"int256","name":"dart","type":"int256"}],"name":"frob","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"},{"internalType":"address","name":"","type":"address"}],"name":"gem","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"i","type":"bytes32"},{"internalType":"address","name":"u","type":"address"},{"internalType":"address","name":"v","type":"address"},{"internalType":"address","name":"w","type":"address"},{"internalType":"int256","name":"dink","type":"int256"},{"internalType":"int256","name":"dart","type":"int256"}],"name":"grab","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"rad","type":"uint256"}],"name":"heal","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"hope","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"ilks","outputs":[{"internalType":"uint256","name":"Art","type":"uint256"},{"internalType":"uint256","name":"rate","type":"uint256"},{"internalType":"uint256","name":"spot","type":"uint256"},{"internalType":"uint256","name":"line","type":"uint256"},{"internalType":"uint256","name":"dust","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"}],"name":"init","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"live","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"rad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"nope","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"rely","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"sin","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"address","name":"usr","type":"address"},{"internalType":"int256","name":"wad","type":"int256"}],"name":"slip","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"u","type":"address"},{"internalType":"address","name":"v","type":"address"},{"internalType":"uint256","name":"rad","type":"uint256"}],"name":"suck","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"},{"internalType":"address","name":"","type":"address"}],"name":"urns","outputs":[{"internalType":"uint256","name":"ink","type":"uint256"},{"internalType":"uint256","name":"art","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"vice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"wards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]"""
        vat_contract = chain.eth.contract(address=Web3.toChecksumAddress(vat_address), abi=vat_abi)

        counter = 0
        for urn in u['urns']:

            if u['urns'][urn]['art'] != 0:
                counter += 1

                ink, art = vat_contract.functions.urns(u['urns'][urn]['hex_ilk'], Web3.toChecksumAddress(urn)).call(block_identifier=var_obj[1])
                a = art / 10**18
                i = ink / 10**18
                if (u['urns'][urn]['art'] - a) > 0.000001:
                    raise AirflowFailException(f"""ERROR: {urn}; {u['urns'][urn]['art']} != {a}""")
                if (u['urns'][urn]['ink'] - i) > 0.000001:
                    raise AirflowFailException(f"""ERROR: {urn}; {u['urns'][urn]['ink']} != {i}""")

        return
    
    qa()

prod_vaults_at_risk_qa = prod_vaults_at_risk_qa()