from web3 import Web3
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.chain import chain


def read_transaction(txhash):

    tx = chain.eth.getTransaction(txhash)

    transaction = dict()
    transaction['hash'] = tx.hash.hex()
    transaction['blockNumber'] = tx.blockNumber
    transaction['gas_price'] = tx.gasPrice
    transaction['from'] = tx['from'].lower()
    if "to" in tx and tx["to"]:
        transaction['to'] = tx.to.lower()
    else:
        transaction['to'] = None
    transaction['value'] = str(tx.value)
    transaction['input'] = tx.input

    return transaction


def is_proxy(proxy):
    abi = '[{"constant":true,"inputs":[],"name":"cache","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]'
    try:
        proxy_contract = chain.eth.contract(address=Web3.toChecksumAddress(proxy), abi=abi)
        cache = proxy_contract.functions.cache().call().lower()
    except:
        cache = None

    return cache and cache == '0x271293c67e2d3140a0e9381eff1f9b01e07b0795'


def is_user_wallet(proxy):
    abi = '[{"constant":true,"inputs":[],"name":"registry","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]'
    try:
        proxy_contract = chain.eth.contract(address=Web3.toChecksumAddress(proxy), abi=abi)
        registry = proxy_contract.functions.registry().call().lower()
    except:
        registry = None

    return registry and registry == '0x498b3bfabe9f73db90d252bcd4fa9548cd0fd981'


def is_instaAccount(proxy):
    abi = '[{"inputs":[],"name":"instaIndex","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]'
    try:
        contract = chain.eth.contract(address=Web3.toChecksumAddress(proxy), abi=abi)
        registry = contract.functions.instaIndex().call().lower()
        instaAccount = registry is not None
    except:
        instaAccount = False

    return instaAccount

def _get_origins():

    query = '''select outputs[0]['value'] as id, tx_hash, from_address, breadcrumb, timestamp, arguments[0]['value']::string as ilk
            from mcd.staging.manager
            where function = 'open' and status = 1 and outputs[0]['value'] not in (select vault from mcd.internal.origins)
            order by timestamp; '''

    rows = sf.execute(query).fetchall()
    output = list()

    for row in rows:
        row = list(row)
        tx = read_transaction(row[1])
        row[3] = row[3].replace("_", "|")

        name = 'DSProxy' if is_proxy(tx['to']) else tx['to']
        if name == 'DSProxy' and row[3] in ('000|000', '000|001'):
            method = 'MakerDAO'
        elif name in ('0x2a35fa541a8481b3a05609f01096d546c26b4a2d', '0x094766d0c35300c1fc2d8a7da8d641886df0e5fd',
                    '0x131e3ca6588f144bb5c04f76167000b906d01b57', '0x6a2c0f859a9f36dce0d8245d7f8d36913b690fbc',
                    '0x275530dd4cb0e305cb8cb03fa34b898bd09936fc'):
            method = 'batch process'
        elif name in ('0x5ef30b9986345249bc32d8928b7ee64de9435e39', '0x82ecd135dce65fbc6dbdd0e4237e0af93ffd5038') and row[3] == '000':
            method = 'MakerDAO'
        elif name == 'DSProxy' and row[3] in ('000|007|009', '000|010|009', '000|022|009', '000|025|009', '000|027|009'):
            method = 'Migrate by MakerDAO'
        elif row[3] in ('003|000|003|000', '003|000|002|000'):
            method = 'Migrate by 1-inch Leverage'
        elif name == 'DSProxy' and row[3] == '000|009|000|004|018':
            method = 'CollateralSwap'
        elif name == 'DSProxy' and row[3] in ('000|009|009', '000|013|009', '000|014|009', '000|015|009', '000|016|009',
                                            '000|024|009', '000|026|009', '000|029|009', '002|001|015|009', '002|001|024|009',
                                            '000|001|000|004|005|000', '000|001|000|004|007|000'):
            method = 'Migrate by DefiSaver'
        elif name == '0x22953b20ab21ef5b2a28c1bb55734fb2525ebaf2':
            method = 'Migrate by DefiSaver'
        elif name == 'DSProxy' or name == '0xa483cfe6403949bf38c74f8c340651fb02246d21':
            method = 'DefiSaver'
        elif name in ('0x57805e5a227937bac2b0fdacaa30413ddac6b8e1', '0x17e8ca1b4798b97602895f63206afcd1fc90ca5f'):
            method = 'Furucombo'
        elif (is_user_wallet(tx['to']) and row[3] == '001|001') or is_instaAccount(tx['to']):
            method = 'InstaDapp'
        elif is_user_wallet(tx['to']):
            method = 'Migrate by InstaDapp'
        elif row[3] == '000|000|000|000':
            method = 'Gnosis Safe'
        elif row[3] == '002|000|000|001|000|000':
            method = 'MyKey'
        elif row[3] in ('002|004|000|000|000|000', '004|002|000|000|000|000') or \
            name in ('0x2b6d87f12b106e1d3fa7137494751566329d1045', '0xcd23f51912ea8fff38815f628277731c25c7fb02', '0x10a0847c2d170008ddca7c3a688124f493630032'):
            method = 'Argent'
        elif name == '0xd70d5fb9582cc3b5b79bbfaecbb7310fd0e3b582':
            method = 'Gelato'
        elif name == '0xe915058df18e7efe92af5c44df3f575fba061b64':
            method = 'Loopring'
        elif name == '0xd75fa26bf6226d64cbc46f6d4e106bcfb43f1670':
            method = 'AaveMakerStrategy'
        else:
            method = 'Unknown'

        # print([str(row[0]), row[5], row[4].__str__()[:19], row[1], method, tx['from']])

        sf.execute(f"""
            INSERT INTO MCD.INTERNAL.ORIGINS(VAULT, ILK, TIMESTAMP, TX_HASH, ORIGIN, WALLET, OWNER)
            VALUES('{str(row[0])}', '{row[5]}', '{row[4].__str__()[:19]}', '{row[1]}', '{method}', '{tx['from']}', '{row[2]}');
        """)
    
    return
