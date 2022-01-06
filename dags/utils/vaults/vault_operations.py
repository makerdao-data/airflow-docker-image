import os, sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import _write_to_stage, sf
from operator import itemgetter


def _vault_operations(vat_operations, manager_operations, opened_vaults, **setup):
    records = list()

    for order_index, block, timestamp, tx_hash, urn, ilk, dink, sink, dart, sart, function in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11
        from @mcd.staging.vaults_extracts/{vat_operations} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2, t.$1;
    """).fetchall():

        block = int(block)
        dink = int(dink)
        sink = int(sink)
        dart = int(dart)
        sart = int(sart)

        vault = opened_vaults[urn] if urn in opened_vaults else urn[:10]

        if function == 'frob' and dink != 0:
            if dink > 0:
                operation = 'DEPOSIT'
            elif dink < 0:
                operation = 'WITHDRAW'
            else:
                operation = 'OTHER'

            record = [
                order_index,
                block,
                timestamp,
                tx_hash,
                'MIGRATION' if vault == '0xc73e0383' else vault,
                ilk,
                urn,
                function,
                dink,
                0,
                operation,
                sink,
                sart,
            ]

            records.append(record)

        if function == 'frob' and dart != 0:

            if dart > 0:
                operation = 'GENERATE'
            elif dart < 0:
                operation = 'PAYBACK'
            else:
                operation = 'OTHER'

            record = [
                order_index,
                block,
                timestamp,
                tx_hash,
                'MIGRATION' if vault == '0xc73e0383' else vault,
                ilk,
                urn,
                function,
                0,
                dart,
                operation,
                sink,
                sart,
            ]

            records.append(record)

        if function != 'frob' and dink != 0 and dart != 0:

            if function == 'grab':
                operation = 'LIQUIDATE'
            elif function == 'fork':
                operation = 'MOVE'
            else:
                operation = 'OTHER'

            record = [
                order_index,
                block,
                timestamp,
                tx_hash,
                'MIGRATION' if vault == '0xc73e0383' else vault,
                ilk,
                urn,
                function,
                dink,
                dart,
                operation,
                sink,
                sart,
            ]

            records.append(record)

        else:

            pass

    for order_index, block, timestamp, tx_hash, ilk, urn, owner, vault, function in sf.execute(f"""
        select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9
        from @mcd.staging.vaults_extracts/{manager_operations} ( FILE_FORMAT => mcd.staging.mcd_file_format ) t
        order by t.$2, t.$1;
    """).fetchall():

        block = int(block)

        record = [
            order_index,
            block,
            timestamp,
            tx_hash,
            'MIGRATION' if vault == '0xc73e0383' else vault,
            ilk,
            urn,
            function,
            0,
            0,
            'OPEN',
            1,
            1,
        ]

        records.append(record)

    ordered_records = sorted(records, key=itemgetter(0))

    pattern = None
    if records:
        pattern = _write_to_stage(sf, ordered_records, f"{setup['db']}.staging.vaults_extracts")

    return pattern
