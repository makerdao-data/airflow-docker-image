from dags.utils.bq_adapters import extract_calls
from dags.utils.bc_adapters import read_urn


def _fetch_manager(**setup):

    calls = extract_calls(
        start_block=setup['start_block'] + 1,
        end_block=setup['end_block'],
        start_time=setup['start_time'],
        end_time=setup['end_time'],
        contract=(setup['manager_address'],),
        abi=setup['manager_abi'],
    )
    records = []
    for call in calls:
        if call.function != '':
            if call.function == 'open' and call.status == 1 and len(call.outputs) > 0:
                urn = read_urn(call.outputs[0]['value'], call.block_number)
                call.outputs.append({'name': 'urn', 'type': 'address', 'value': urn})
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
            ]

            records.append(record)

    print(f"""CDP calls: {len(calls)} read, {len(records)} prepared to write""")

    return records
