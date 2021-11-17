from operator import itemgetter


def _vault_operations(vat_operations, manager_operations, opened_vaults):
    records = list()

    for order_index, block, timestamp, tx_hash, urn, ilk, dink, sink, dart, sart, function in vat_operations:

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

    for order_index, block, timestamp, tx_hash, ilk, urn, owner, vault, function in manager_operations:

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

    return ordered_records
