import json
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
    ) in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16   
        from @mcd.staging.vaults_extracts/{manager} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():

        status = int(status)
        outputs = json.loads(outputs.replace("\'", "\""))

        if status == 1 and function == 'open':

            opened_vaults[outputs[1]['value']] = outputs[0]['value']

    return opened_vaults
