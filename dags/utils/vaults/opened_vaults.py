import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf


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

    pattern = None
    if opened_vaults:
        pattern = _write_to_stage(sf, opened_vaults, f"{setup['db']}.staging.vaults_extracts")

    return pattern
