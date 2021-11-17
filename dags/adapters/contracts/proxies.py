from dags.connectors.chain import chain
from web3 import Web3


def read_code_hash(contract_address):
    try:
        byte_code = chain.eth.getCode(Web3.toChecksumAddress(contract_address))
        return Web3.sha3(byte_code).hex()

    except Exception as ex:
        print(f'Reading code hash for contract {contract_address} failed.')
        print(ex)
        return None


def read_vote_proxy(proxy_address):

    abi = '''
          [{"constant":true,"inputs":[],"name":"gov","outputs":[{"name":"","type":"address"}],
          "payable":false,"stateMutability":"view","type":"function"},
          {"constant":true,"inputs":[],"name":"cold","outputs":[{"name":"","type":"address"}],
          "payable":false,"stateMutability":"view","type":"function"}, 
          {"constant":false,"inputs":[],"name":"freeAll","outputs":[],
          "payable":false,"stateMutability":"nonpayable","type":"function"},
          {"constant":true,"inputs":[],"name":"iou","outputs":[{"name":"","type":"address"}],
          "payable":false,"stateMutability":"view","type":"function"},
          {"constant":true,"inputs":[],"name":"hot","outputs":[{"name":"","type":"address"}],
          "payable":false,"stateMutability":"view","type":"function"},
          {"constant":true,"inputs":[],"name":"chief","outputs":[{"name":"","type":"address"}],
          "payable":false,"stateMutability":"view","type":"function"}] 
          '''

    try:
        proxy_contract = chain.eth.contract(address=Web3.toChecksumAddress(proxy_address), abi=abi)
        chief = proxy_contract.functions.chief().call()
        gov = proxy_contract.functions.gov().call()
        iou = proxy_contract.functions.iou().call()
        cold = proxy_contract.functions.cold().call()
        hot = proxy_contract.functions.hot().call()

    except Exception as ex:
        print(f'Reading data for contract {proxy_address} failed.')
        print(ex)
        chief = gov = iou = cold = hot = None

    return chief, gov, iou, cold, hot
