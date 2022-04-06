from web3 import Web3
from airflow.exceptions import AirflowFailException
import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.chain import chain


def _debt_calculation(**setup):

    ABI = """[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":true,"inputs":[{"indexed":true,"internalType":"bytes4","name":"sig","type":"bytes4"},{"indexed":true,"internalType":"bytes32","name":"arg1","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg2","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg3","type":"bytes32"},{"indexed":false,"internalType":"bytes","name":"data","type":"bytes"}],"name":"LogNote","type":"event"},{"constant":true,"inputs":[],"name":"Line","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"cage","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"can","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"dai","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"debt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"deny","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"bytes32","name":"what","type":"bytes32"},{"internalType":"uint256","name":"data","type":"uint256"}],"name":"file","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"what","type":"bytes32"},{"internalType":"uint256","name":"data","type":"uint256"}],"name":"file","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"flux","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"i","type":"bytes32"},{"internalType":"address","name":"u","type":"address"},{"internalType":"int256","name":"rate","type":"int256"}],"name":"fold","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"int256","name":"dink","type":"int256"},{"internalType":"int256","name":"dart","type":"int256"}],"name":"fork","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"i","type":"bytes32"},{"internalType":"address","name":"u","type":"address"},{"internalType":"address","name":"v","type":"address"},{"internalType":"address","name":"w","type":"address"},{"internalType":"int256","name":"dink","type":"int256"},{"internalType":"int256","name":"dart","type":"int256"}],"name":"frob","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"},{"internalType":"address","name":"","type":"address"}],"name":"gem","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"i","type":"bytes32"},{"internalType":"address","name":"u","type":"address"},{"internalType":"address","name":"v","type":"address"},{"internalType":"address","name":"w","type":"address"},{"internalType":"int256","name":"dink","type":"int256"},{"internalType":"int256","name":"dart","type":"int256"}],"name":"grab","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"rad","type":"uint256"}],"name":"heal","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"hope","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"ilks","outputs":[{"internalType":"uint256","name":"Art","type":"uint256"},{"internalType":"uint256","name":"rate","type":"uint256"},{"internalType":"uint256","name":"spot","type":"uint256"},{"internalType":"uint256","name":"line","type":"uint256"},{"internalType":"uint256","name":"dust","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"}],"name":"init","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"live","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"rad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"nope","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"}],"name":"rely","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"sin","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes32","name":"ilk","type":"bytes32"},{"internalType":"address","name":"usr","type":"address"},{"internalType":"int256","name":"wad","type":"int256"}],"name":"slip","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"u","type":"address"},{"internalType":"address","name":"v","type":"address"},{"internalType":"uint256","name":"rad","type":"uint256"}],"name":"suck","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"},{"internalType":"address","name":"","type":"address"}],"name":"urns","outputs":[{"internalType":"uint256","name":"ink","type":"uint256"},{"internalType":"uint256","name":"art","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"vice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"wards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]"""

    ilks = sf.execute(f"""
        SELECT DISTINCT ILK
        FROM {setup['db']}.INTERNAL.ILKS;
    """
    ).fetchall()

    mcd_vat = chain.eth.contract(
        address=Web3.toChecksumAddress("0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B"), abi=ABI
    )

    max_block = sf.execute(f"""
        SELECT MAX(BLOCK)
        FROM {setup['db']}.STAGING.BLOCKS;
    """
    ).fetchone()[0]

    d = 0
    for ilk in ilks:

        name = ilk[0]
        ilk = ilk[0]

        
        ilk_encoded = '0x' + bytes(ilk, 'utf-8').hex()
        ilk = ilk_encoded.ljust(44, '0')

        x = mcd_vat.functions.ilks(ilk).call(block_identifier=max_block)
        # x = mcd_vat.functions.ilks(ilk).call()

        """
        Art
        rate
        spot
        line
        dust
        """

        Art, rate, spot, line, dust = x

        debt = (Art/10**18) * (rate / 10**27)
        # print(f"""{name}: {debt}""")
        d += debt

    db_debt = sf.execute(f"""
        SELECT sum(debt)
        FROM {setup['db']}.PUBLIC.CURRENT_VAULTS;
    """
    ).fetchone()[0]

    # Output current debts on-chain vs in-db.
    print(f"""in-db debt : {round(db_debt, 7)}\non-chain debt : {round(d, 7)}""")
    
    # Raise airflow exception if difference between debts exceeds |0.1|.
    # Otherwise, they are assumed to be quasi-equivalent, therefor correct.
    calc = db_debt - d
    if not (-.1 < calc < 0.1):
        raise AirflowFailException(f'WARNING: DEBT IS NOT CORRECT')
    
    try:
        sin = mcd_vat.functions.sin(
            Web3.toChecksumAddress("0xA950524441892A31ebddF91d3cEEFa04Bf454466")
        ).call(block_identifier=max_block)

        sin = sin / 10**45
        
    except:
        sin = 0
    
    sf.execute(f"""
        INSERT INTO {setup['db']}.internal.vow(timestamp, sin)
        VALUES('{setup['end_time']}', {sin});
    """
    )

    return
