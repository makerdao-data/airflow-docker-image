from dags.connectors.sf import sf


def _fetch_oracles(**setup):

    all_ilks = sf.execute(
        f"""
        SELECT *
        FROM {setup['db']}.INTERNAL.ILKS
        WHERE PIP_ORACLE_ADDRESS IS NOT NULL;
    """
    ).fetchall()

    oracles = dict()

    for i in all_ilks:
        if i[0] != 'SAI':
            oracle_address = i[5]
            if i[0].split('-') == 3:
                ilk = i[0].split('-')[1]
            else:
                ilk = i[0].split('-')[0]

            if oracle_address.lower() in oracles:
                if ilk not in oracles[oracle_address.lower()]:
                    oracles[oracle_address.lower()].append(ilk)
            else:
                oracles[oracle_address.lower()] = [ilk]

    return oracles
