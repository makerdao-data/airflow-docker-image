from airflow.exceptions import AirflowFailException
import sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf


def _cohesion_check(blocks, vat, manager, rates, prices, **setup):

    test = []
    if blocks:
        # BLOCKS
        last_block = sf.execute(
            f"""
            SELECT max(block)
            FROM {setup['db']}.staging.blocks; """
        ).fetchone()[0]

        test = []
        if last_block == blocks[0][1] - 1:
            counter = 0
            while counter < len(blocks) - 1:
                test.append(blocks[counter][1] == blocks[counter + 1][1] - 1)
                counter += 1

    blocks_test = all(test)
    print(f'Blocks test result: {blocks_test}')

    # RATES DICT
    rates_dict = dict()
    for i in rates:
        rates_dict.setdefault(i[1], [])
        if i[3] not in rates_dict[i[1]]:
            rates_dict[i[1]].append(i[3])

    print(rates_dict)

    # PRICES DICT
    prices_dict = dict()
    for i in prices:
        prices_dict.setdefault(i[1], [])
        if i[3] not in prices_dict[i[1]]:
            prices_dict[i[1]].append(i[3])

    print(prices_dict)

    ### VAULT OPERATIONS

    test = []
    if manager:

        # CDP MANAGER
        for i in manager:
            if i[10] == 'open' and i[14] == 1:

                if i[11][0]['value'] == '4554482d410000000000000000000000':
                    collateral = 'ETH-A'
                elif i[11][0]['value'] == '4241542d410000000000000000000000':
                    collateral = 'BAT-A'
                else:
                    collateral = i[11][0]['value']

                if (
                    i[1] in rates_dict
                    and collateral in rates_dict[i[1]]
                    and i[1] in prices_dict
                    and (collateral.split('-')[0] in prices_dict[i[1]] or collateral.split('-')[1] in prices_dict[i[1]])
                ):

                    test.append(True)

                else:

                    print(f'MANAGER: {i}')
                    test.append(False)

    if vat:
        # VAT
        for i in vat:
            if i[10] in ('frob', 'grab, fork') and i[14] == 1:

                if i[11][0]['value'] == '4554482d410000000000000000000000':
                    collateral = 'ETH-A'
                elif i[11][0]['value'] == '4241542d410000000000000000000000':
                    collateral = 'BAT-A'
                else:
                    collateral = i[11][0]['value']

                if (
                    i[1] in rates_dict
                    and collateral in rates_dict[i[1]]
                    and i[1] in prices_dict
                    and (collateral.split('-')[0] in prices_dict[i[1]] or collateral.split('-')[1] in prices_dict[i[1]])
                ):

                    test.append(True)

                else:

                    print(f'VAT: {i}')
                    test.append(False)

    vault_operations_test = all(test)
    print(f'Vault operations test result: {vault_operations_test}')

    test_result = all([blocks_test, vault_operations_test])

    if not test_result:

        raise AirflowFailException("#ERROR: COHESION CHECK FAILED. SHUTTING DOWN THE PROCESS")

    return test_result
