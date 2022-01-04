import os, sys
sys.path.append('/opt/airflow/')
import json
import requests
from datetime import datetime, timezone
from dags.connectors.sf import _write_to_stage, sf


def timestamp_converter(d: str) -> int:

    dt = datetime.strptime(d, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    t = int(datetime.timestamp(dt))

    return t


def _fetch_external_prices(**setup):

    s = timestamp_converter(setup['start_time'])
    e = timestamp_converter(setup['end_time'])

    external_prices = {}
    gecko_coins = {
        'AAVE': {'id': 'aave'},
        'TUSD': {'id': 'true-usd'},
        'KNC': {'id': 'kyber-network-crystal'},
        'USDT': {'id': 'tether'},
        'PAXUSD': {'id': 'paxos-standard'},
        'WBTC': {'id': 'wrapped-bitcoin'},
        'COMP': {'id': 'compound-governance-token'},
        'ETH': {'id': 'ethereum'},
        'BAT': {'id': 'basic-attention-token'},
        'USDC': {'id': 'usd-coin'},
        'ZRX': {'id': '0x'},
        'MANA': {'id': 'decentraland'},
        'LRC': {'id': 'loopring'},
        'LINK': {'id': 'chainlink'},
        'BAL': {'id': 'balancer'},
        'YFI': {'id': 'yearn-finance'},
        'GUSD': {'id': 'gemini-dollar'},
        'UNI': {'id': 'uniswap'},
        'RENBTC': {'id': 'renbtc'},
        'MATIC': {'id': 'matic-network'},
    }

    for coin in gecko_coins:

        c = gecko_coins[coin]['id']
        r = requests.get(
            f"""https://api.coingecko.com/api/v3/coins/{c}/market_chart/range?vs_currency=usd&from={s}&to={e}"""
        )
        # r = requests.get(f"""https://api.coingecko.com/api/v3/coins/{c}/market_chart?vs_currency=usd&days=1""")
        if r.status_code == 200:
            resp = json.loads(r.text)
            external_prices[coin] = resp['prices']
        else:
            external_prices[coin] = None
        
    pattern = None
    if external_prices:
        pattern = _write_to_stage(sf, external_prices, f"{setup['db']}.staging.vaults_extracts")

    return pattern
