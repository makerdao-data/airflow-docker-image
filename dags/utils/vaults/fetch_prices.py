from decimal import Decimal
from datetime import datetime, timedelta
from dags.connectors.sf import sf
from dags.utils.bq_adapters import extract_pip_events
from dags.connectors.coingecko import get_gecko_price


def _fetch_prices(new_blocks, oracles, external_prices, **setup):

    prices = dict()
    for row in sf.execute(
        f"""
        SELECT distinct token, last_value(osm_price) over (partition by token order by block),
            last_value(mkt_price) over (partition by token order by block)
        FROM {setup['db']}.internal.prices; """
    ):
        prices[row[0]] = (row[1], row[2])

    tokens = dict()
    for ilk in sf.execute(
        f"""
        SELECT ilk, block, timestamp, cp_id, pip_oracle_name, pip_oracle_address, type, abi_hash
        FROM {setup['db']}.internal.ilks
        ORDER BY block desc; """
    ):

        ilk_parts = ilk[0].split('-')
        if len(ilk_parts) == 3:
            token = ilk_parts[1]
        else:
            token = ilk_parts[0]

        tokens[token] = [ilk[2], ilk[3], None, None]
        if token in ('SAI', 'USDC', 'TUSD', 'USDT', 'PAXUSD', 'GUSD'):
            tokens[token][3] = 1
        elif token in prices:
            tokens[token][3] = prices[token][0]

        if token in prices and prices[token][1]:
            tokens[token][2] = prices[token][1]

    blocks = []
    for b in new_blocks:
        if b[1] > setup['start_block']:
            blocks.append([b[1], b[2]])

    signatures = [
        '0x296ba4ca62c6c21c95e828080cb8aec7481b71390585605300a8a76f9e95b527',
        '0x80a5d0081d7e9a7bdb15ef207c6e0772f0f56d24317693206c0e47408f2d0b73',
    ]

    operations = extract_pip_events(
        setup['start_block'] + 1,
        setup['start_time'],
        setup['end_block'],
        setup['end_time'],
        oracles,
        signatures,
    )

    records = []
    pointer = 0
    for block in blocks:
        while pointer < len(operations) and operations[pointer]['block'] == block[0]:
            tokens[operations[pointer]['ilk']][3] = operations[pointer]['price']
            pointer += 1
        for token in tokens:

            temp_block_timestamp = datetime.strptime(block[1], '%Y-%m-%d %H:%M:%S')
            if tokens[token][0] - temp_block_timestamp < timedelta(minutes=5):

                osm_price = tokens[token][3]

                if token == 'SAI':
                    market_price = 1
                else:
                    try:
                        market_price = get_gecko_price(
                            int(datetime.timestamp(temp_block_timestamp)), token, external_prices
                        )
                    except Exception as e:
                        print(e)
                        market_price = None

                if not market_price:
                    market_price = tokens[token][2]
                else:
                    tokens[token][2] = market_price

                if type(market_price) == Decimal:
                    market_price = str(market_price)

                if type(osm_price) == Decimal:
                    osm_price = str(osm_price)

                records.append((setup['load_id'], block[0], block[1], token, market_price, osm_price))

    print(f"""Prices: {len(operations)} read, {len(records)} written""")

    return records
