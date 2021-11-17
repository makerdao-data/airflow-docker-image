from dags.utils.bq_adapters import extract_calls_gas


def _fetch_chief(**setup):

    chief = []
    read = 0
    for chief_address in setup['chief_addresses']:

        # start_block, end_block, start_time, end_time, contract, abi
        calls = extract_calls_gas(
            setup['start_block'] + 1,
            setup['end_block'],
            setup['start_time'],
            setup['end_time'],
            (chief_address,),
            setup['chief_abi'],
        )

        chief += [
            [
                setup['load_id'],
                call.block_number,
                call.timestamp.__str__()[:19],
                call.breadcrumb,
                call.tx_hash,
                call.tx_index,
                call.type,
                str(call.value),
                call.from_address,
                call.to_address,
                call.function,
                call.arguments,
                call.outputs,
                call.error,
                call.status,
                call.gas_used,
                call.gas_price,
            ]
            for call in calls
            if call.function != ''
        ]
        read += len(calls)

    print(f"CHIEF: {read} read, {len(chief)} prepared to upload")

    return chief
