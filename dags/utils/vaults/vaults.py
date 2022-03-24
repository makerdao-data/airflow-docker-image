import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf


def _vaults(mats, vault_operations, rates, prices, **setup):

    prices_dict = dict()
    for load_id, block, timestamp, token, market_price, osm_price in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
        from @mcd.staging.vaults_extracts/{prices} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():

        block = int(block)
        if market_price and market_price != 'None':
            market_price = float(market_price)
        if osm_price and osm_price != 'None':
            osm_price = float(osm_price)

        prices_dict.setdefault(block, {})
        prices_dict[block][token] = [load_id, block, timestamp, token, market_price, osm_price]

    rates_dict = dict()
    for load_id, block, timestamp, ilk, rate in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5
        from @mcd.staging.vaults_extracts/{rates} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall():

        block = int(block)
        rate = int(rate)

        rates_dict.setdefault(block, {})
        rates_dict[block][ilk] = [load_id, block, timestamp, ilk, rate]

    admins_dict = dict()
    admins = sf.execute(f"""
        SELECT URN, OWNER, DS_PROXY
        FROM {setup['db']}.public.admin;
    """).fetchall()

    for urn, owner, ds_proxy in admins:
        
        if urn:
            admins_dict.setdefault(urn.lower(), {})
            admins_dict[urn.lower()] = dict(
                owner = owner.lower() if owner else None,
                ds_proxy = ds_proxy.lower() if ds_proxy else None
            )

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
    ) in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12, t.$13
        from @mcd.staging.vaults_extracts/{vault_operations} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2, t.$1;
    """).fetchall():

        block = int(block)
        dink = int(dink)
        sink = int(sink)
        dart = int(dart)
        sart = int(sart)

        if ilk == '4554482d410000000000000000000000':
            ilk = 'ETH-A'
        elif ilk == '4241542d410000000000000000000000':
            ilk = 'BAT-A'
        else:
            ilk = ilk

        if ilk[:4] != 'RWA0':
            c = ilk.split('-')[0] if ilk.split('-')[0] in prices_dict[block] else ilk.split('-')[1]
            mkt = prices_dict[block][c][4] if 'SAI' not in ilk else 1
            orc = prices_dict[block][c][5] if 'SAI' not in ilk else 1
        else:
            mkt = None
            orc = None

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
            mkt,
            orc,
            rates_dict[block][ilk][4],
            sink,
            sart,
            urn,
        ]

        public_vaults.append(x)

    # sorted(public_vaults, key=lambda x: (x[0], x[6]))

    db_mats = sf.execute(
        f"""
        select load_id, block, timestamp, ilk, mat
        from {setup['db']}.internal.mats
        order by block;
    """
    ).fetchall()

    temp_mats = sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5
        from @mcd.staging.vaults_extracts/{mats} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2;
    """).fetchall()

    to_int_temp_mats = []
    for load_id, block, timestamp, ilk, mat in temp_mats:
        to_int_temp_mats.append(
            [
                load_id,
                int(block),
                timestamp,
                ilk,
                float(mat),
            ]
        )

    all_mats = db_mats + to_int_temp_mats
    sorted(all_mats, key=lambda x: x[1])

    vaults_summary = sf.execute(f"""
        select vault, round(sum(dcollateral), 8), sum(dart), round(sum(dprincipal), 8), sum(dfees)
        from {setup['db']}.public.vaults
        group by vault;
    """
    ).fetchall()

    vaults = dict()
    for vault, collateral, art, principal, fees in vaults_summary:
        vaults[vault] = dict(collateral=collateral, art=art, principal=principal, sf_paid=fees)

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
            if int(mat_change[1]) <= int(row[1]):
                if mat_change[3] == row[5]:
                    mat = round(float(mat_change[4]), 6)
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
                admins_dict[urn]['ds_proxy'] if urn in admins_dict else None,
                admins_dict[urn]['owner'] if urn in admins_dict else None
            ]
        )

    print(f"""Vaults related records to be loaded to public.vaults: {len(vaults_table_records)}""")

    pattern = None
    if vaults_table_records:
        pattern = _write_to_stage(sf, vaults_table_records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
