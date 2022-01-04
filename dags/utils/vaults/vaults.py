import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf


def _vaults(mats, vault_operations, rates, prices, **setup):

    prices_dict = dict()
    for load_id, block, timestamp, token, market_price, osm_price in prices:

        prices_dict.setdefault(block, {})
        prices_dict[block][token] = [load_id, block, timestamp, token, market_price, osm_price]

    rates_dict = dict()
    for load_id, block, timestamp, ilk, rate in rates:

        rates_dict.setdefault(block, {})
        rates_dict[block][ilk] = [load_id, block, timestamp, ilk, rate]

    public_vaults = list()
    for (
        order_index,
        block,
        timestamp,
        tx_hash,
        vault,
        ilk,
        urn,
        function,
        dink,
        dart,
        operation,
        sink,
        sart,
    ) in vault_operations:

        c = ilk.split('-')[0] if ilk.split('-')[0] in prices_dict[block] else ilk.split('-')[1]
        x = [
            order_index,
            block,
            timestamp,
            tx_hash,
            vault,
            ilk,
            operation,
            dink,
            dart,
            prices_dict[block][c][4] if 'SAI' not in ilk else 1,
            prices_dict[block][c][5] if 'SAI' not in ilk else 1,
            rates_dict[block][ilk][4],
            sink,
            sart,
            urn,
        ]

        public_vaults.append(x)

    sorted(public_vaults, key=lambda x: x[0])

    db_mats = sf.execute(
        f"""
        select load_id, block, timestamp, ilk, mat
        from {setup['db']}.internal.mats
        order by block;
    """
    ).fetchall()

    all_mats = db_mats + mats
    sorted(all_mats, key=lambda x: x[1])

    vaults = dict()
    vaults_table_records = []
    for row in public_vaults:
        vault = row[4]
        if vault not in vaults:
            vaults[vault] = dict(collateral=0, art=0, principal=0, sf_paid=0)

        dink = int(row[12]) * int(row[7])
        dart = int(row[13]) * int(row[8])
        rate = int(row[11])

        ddebt = dart * rate / 10 ** 45
        dprincipal = max(ddebt, -vaults[vault]['principal'])
        dfees = min(ddebt - dprincipal, 0)

        vaults[vault]['collateral'] += dink
        vaults[vault]['art'] += dart
        vaults[vault]['principal'] += dprincipal
        vaults[vault]['sf_paid'] += -dfees

        mat = 0
        for mat_change in all_mats:
            if mat_change[1] <= row[1]:
                if mat_change[3] == row[5]:
                    mat = round(mat_change[4], 6)
            else:
                break

        vaults_table_records.append(
            [
                load_id,
                *row[:7],
                dink / 10 ** 18,
                dprincipal,
                -dfees or 0,
                row[9],
                row[10],
                dart,
                row[11],
                mat,
                row[14],
            ]
        )

    print(f"""Vaults related records to be loaded to public.vaults: {len(vaults_table_records)}""")

    pattern = None
    if vaults_table_records:
        pattern = _write_to_stage(sf, vaults_table_records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
