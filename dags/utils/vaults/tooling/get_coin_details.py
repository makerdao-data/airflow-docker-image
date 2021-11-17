import json
import requests
import hashlib
from config import ETHERSCAN_API_KEY


def _get_coin_details(pip_oracle_address, coin_types):

    req = requests.get(
        'https://api.etherscan.io/api?module=contract&action=getabi&address='
        + pip_oracle_address
        + '&apikey='
        + ETHERSCAN_API_KEY
    )

    resp = json.loads(req.text)
    abi = resp['result']
    hash_object = hashlib.md5(str(abi).encode())
    md5_hash = hash_object.hexdigest()

    try:
        coin_type = coin_types[md5_hash]
    except:
        coin_type = None

    return md5_hash, coin_type
