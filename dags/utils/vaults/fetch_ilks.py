import json
import requests
from dags.connectors.sf import sf
from dags.utils.vaults.tooling.get_coin_details import _get_coin_details


def _fetch_ilks(vat, **setup):

    # get all registered ilks
    registered_ilks = sf.execute(
        f"""
        SELECT ilk, block, timestamp, cp_id, pip_oracle_name, pip_oracle_address, type, abi_hash
        FROM {setup['db']}.internal.ilks
        WHERE ilk != 'SAI'
    """
    ).fetchall()

    ilks_registry = dict()
    for ilk, block, timestamp, cp_id, pip_oracle_name, pip_oracle_address, type, abi_hash in registered_ilks:
        ilks_registry[ilk] = dict(
            block=block,
            timestamp=timestamp,
            cp_id=cp_id,
            pip_oracle_name=pip_oracle_name,
            pip_oracle_address=pip_oracle_address,
            type=type,
            abi_hash=abi_hash,
        )

    all_inits = sf.execute(
        f"""
        SELECT block, timestamp, cast(arguments[0]['value'] as varchar) as ilk
        FROM {setup['db']}.staging.vat
        WHERE function = 'init'
        ORDER BY timestamp desc, breadcrumb desc;
    """
    ).fetchall()

    # new inits
    new_inits = []
    for i in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16   
        from @mcd.staging.vaults_extracts/{vat} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():
        if i[10] == 'init' and int(i[14]) == 1:
            new_inits.append([i[1], i[2], i[11][0]['value']])

    all_inits = all_inits + new_inits

    # get pip registry
    source = 'https://changelog.makerdao.com/releases/mainnet/active/contracts.json'

    get_changelog = requests.get(source)
    if get_changelog.status_code == 200:

        changelog = json.loads(get_changelog.text)

        pip_oracles = dict()

        for i in changelog:
            if 'PIP_' in i:
                ilk = i[4:]
                pip_oracles[ilk] = changelog[i]

    abi_hashes = sf.execute(
        f"""
        SELECT type, abi_hash
        FROM {setup['db']}.internal.ilks
        WHERE abi_hash is not null;
    """
    ).fetchall()

    coin_types = {
        '7f4551c6281ac51fab270f82bba99304': 'stablecoin',
        '9412a4654f866d3b969e1ceb40e021f6': 'coin',
        '3614ff4a641c4e903e782c94c9049d51': 'coin',
        'd3c0c22021d1b25c9daf85fdfc8f5f7d': 'lp',
    }
    for coin_type, abi_hash in abi_hashes:
        if abi_hash not in coin_types:
            coin_types[abi_hash] = coin_type

    for block, timestamp, ilk in all_inits:

        if ilk != 'SAI':

            record = dict()
            record['ilk'] = ilk
            record['block'] = block
            record['timestamp'] = timestamp

            if ilk not in ilks_registry:

                if ilk[:-2] in pip_oracles:

                    pip_oracle_address = pip_oracles[ilk[:-2]].lower()
                    md5_hash, coin_type = _get_coin_details(pip_oracle_address, coin_types)
                    record['pip_oracle_name'] = 'PIP_' + ilk[:-2]
                    record['pip_oracle_address'] = pip_oracle_address
                    record['coin_type'] = coin_type
                    record['abi_hash'] = md5_hash

                    # write to db
                    sf.execute(
                        f"""
                        INSERT INTO {setup['db']}.internal.ilks(ilk, block, timestamp, pip_oracle_name, pip_oracle_address, type, abi_hash)
                        VALUES ('{record['ilk']}', {record['block']}, '{record['timestamp'].__str__()[:19]}', '{record['pip_oracle_name']}', '{record['pip_oracle_address']}', '{record['coin_type']}', '{record['abi_hash']}');
                    """
                    )

                else:

                    sf.execute(
                        f"""
                        INSERT INTO {setup['db']}.internal.ilks(ilk, block, timestamp)
                        VALUES ('{record['ilk']}', {record['block']}, '{record['timestamp'].__str__()[:19]}');
                    """
                    )

            elif ilk in ilks_registry and ilks_registry[ilk]['pip_oracle_address'] == None:

                if ilk[:-2] in pip_oracles:

                    pip_oracle_address = pip_oracles[ilk[:-2]].lower()
                    md5_hash, coin_type = _get_coin_details(pip_oracle_address, coin_types)

                    record['pip_oracle_name'] = 'PIP_' + ilk[:-2]
                    record['pip_oracle_address'] = pip_oracle_address
                    record['coin_type'] = coin_type
                    record['abi_hash'] = md5_hash

                    # update existing record
                    sf.execute(
                        f"""
                        UPDATE {setup['db']}.internal.ilks
                        SET pip_oracle_name = '{record['pip_oracle_name']}', pip_oracle_address = '{record['pip_oracle_address']}', type = '{record['coin_type']}', abi_hash = '{record['abi_hash']}'
                        WHERE ilk = '{record['ilk']}' and block = {record['block']} and timestamp = '{record['timestamp'].__str__()[:19]}';
                    """
                    )

            else:

                pass

        elif ilk == 'SAI':
            
            if not sf.execute(
                f"""
                SELECT ilk
                FROM mcd.internal.ilks
                where ilk = 'SAI';
            """
            ).fetchone()[0]:

                sf.execute(
                    f"""
                    INSERT INTO {setup['db']}.internal.ilks(ilk, block, timestamp, cp_id, pip_oracle_name, pip_oracle_address, type, abi_hash)
                    VALUES ('{ilk}', {block}, '{timestamp.__str__()[:19]}', 'sai-single-collateral-dai', NULL, NULL, NULL, NULL);
                """
                )

        else:

            pass

    return
