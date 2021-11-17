import json
from dags.connectors.sf import sf


def _opened_vaults(manager, **setup):

    q = f"""SELECT outputs[1].value::string, outputs[0].value
    FROM {setup['db']}.staging.manager
    WHERE function = 'open'; """

    opened_vaults_list = sf.execute(q).fetchall()

    opened_vaults = dict()
    for urn, cdp_id in opened_vaults_list:

        opened_vaults[urn] = cdp_id

    for (
        load_id,
        block,
        timestamp,
        breadcrumb,
        tx_hash,
        tx_index,
        type,
        value,
        from_address,
        to_address,
        function,
        arguments,
        outputs,
        error,
        status,
        gas_used,
    ) in manager:

        if status == 1 and function == 'open':

            opened_vaults[outputs[1]['value']] = outputs[0]['value']

    return opened_vaults
