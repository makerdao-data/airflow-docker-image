import json


def _proc(sf, end_block, end_timestamp):

    data = sf.execute("""
        SELECT URN, MAT, PRICE, RATE
        FROM MAKER.RISK.VAULTS
        WHERE ID = 1;
    """).fetchall()

    vaults_at_risk = list()

    for urns, mats, prices, rates in data:

        urns = json.loads(urns)
        mats = json.loads(mats)
        prices = json.loads(prices)
        rates = json.loads(rates)

        for urn in urns['urns']:

            ILK = urns['urns'][urn]['hex_ilk']
            debt = urns['urns'][urn]['art'] * rates['rates'][ILK]

            # if there's no pip for collateral,
            # there's strong assumption that thic collateral is a stablecoin
            if ILK not in prices['prices']:
                current_price = 1
                next_price = 1
            else:
                current_price = prices['prices'][ILK]['curr']
                next_price = prices['prices'][ILK]['nxt']
            if debt > 0:
                next_collateralization = ((urns['urns'][urn]['ink'] * next_price) / debt) * 100
                mat = mats['mats'][ILK]
                is_at_risk = next_collateralization <= mat
                if is_at_risk and urns['urns'][urn]['ilk'][:4] != 'RWA0':
                    
                    vaults_at_risk.append(
                        dict(
                            urn = urn,
                            debt = debt,
                            collateral = urns['urns'][urn]['ink'],
                            current_collateralization = ((urns['urns'][urn]['ink'] * current_price) / debt) * 100,
                            next_collateralization = next_collateralization,
                            current_price = current_price,
                            next_price = next_price,
                            liquidation_ratio = mat,
                            hex_ilk = ILK,
                            ilk = urns['urns'][urn]['ilk']
                        )
                    )

    output = dict(
        last_scanned_block = end_block,
        latest_timestamp = end_timestamp,
        vaults_at_risk = vaults_at_risk
    )

    sf.execute(f"""
        UPDATE MAKER.RISK.VAULTS
        SET VAULTS = PARSE_JSON($${output}$$)
        WHERE ID = 1;
    """)

    return