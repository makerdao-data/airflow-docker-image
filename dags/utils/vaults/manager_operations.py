import json


def _manager_operations(manager):

    records = list()

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

            operation = [
                str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                block,
                timestamp,
                tx_hash,
                arguments[0]['value'],
                outputs[1]['value'],
                arguments[1]['value'],
                outputs[0]['value'],
                function,
            ]

            records.append(operation)

    return records
