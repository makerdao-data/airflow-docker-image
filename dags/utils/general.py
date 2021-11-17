from web3 import Web3
from dags.connectors.chain import chain


def balance_of(token, address, block=None):

    ABI = """[{"constant": true,"inputs": [],"name": "decimals","outputs": [{"internalType": "uint8","name": "","type": "uint8"}],
           "payable": false,"stateMutability": "view","type": "function"},
          {"constant": true,"inputs": [],"name": "symbol","outputs": [{"internalType": "string","name": "","type": "string"}],
           "payable": false,"stateMutability": "view","type": "function"},
          {"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
           "payable":false,"stateMutability":"view","type":"function"}]"""

    try:
        token_contract = chain.eth.contract(address=Web3.toChecksumAddress(token), abi=ABI)
        if block:
            balance = token_contract.functions.balanceOf(Web3.toChecksumAddress(address)).call(
                block_identifier=block
            )
        else:
            balance = token_contract.functions.balanceOf(Web3.toChecksumAddress(address)).call()
        decimals = token_contract.functions.decimals().call()
        balance /= 10 ** decimals
    except Exception as e:
        print(e)
        balance = None

    return balance
