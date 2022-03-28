from web3 import Web3
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.chain import chain
from dags.utils.weights.tools import _get_chief, _get_deposits, _get_max_block


def _get_eod_deposit(date, vote_delegate):
    
    block = _get_max_block(date)
    chief = _get_chief(chain, vote_delegate)
    deposit = _get_deposits(chain, chief, vote_delegate, block)

    return deposit