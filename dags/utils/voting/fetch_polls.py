from dags.utils.bq_adapters import extract_calls_gas


def _fetch_polls(**setup):

    calls = extract_calls_gas(
        setup['start_block'] + 1,
        setup['end_block'],
        setup['start_time'],
        setup['end_time'],
        (setup['polls_address'],),
        setup['polls_abi'],
    )

    records = [
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

    calls_num = len(calls)

    calls = extract_calls_gas(
        setup['start_block'] + 1,
        setup['end_block'],
        setup['start_time'],
        setup['end_time'],
        (setup['new_polls_address'],),
        setup['new_polls_abi'],
    )

    for call in calls:
        if call.function == 'voteMany':
            for i, poll in enumerate(call.arguments[0]['value']):
                option = call.arguments[1]['value'][i] if i < len(call.arguments[1]['value']) else None
                arguments = [
                    dict(name='pollId', type='uint256', value=poll),
                    dict(name='optionId', type='uint256', value=option),
                ]

                record = [
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
                    'vote',
                    arguments,
                    [],
                    call.error,
                    call.status,
                    call.gas_used / len(call.arguments[0]['value']),
                    call.gas_price,
                ]

                records.append(record)

        elif call.function == 'withdrawPolls':
            for poll in call.arguments[0]['value']:
                arguments = [dict(name='pollId', type='uint256', value=poll)]
                record = [
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
                    'withdrawPoll',
                    arguments,
                    [],
                    call.error,
                    call.status,
                    call.gas_used,
                    call.gas_price,
                ]
                records.append(record)

        else:
            record = [
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
            records.append(record)

    print(f"POLLS: {len(calls) + calls_num} read, {len(records)} written")

    return records
