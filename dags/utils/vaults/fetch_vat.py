from dags.utils.bq_adapters import decode_calls


def _fetch_vat(**setup):

    records = decode_calls(
        contract=(setup['vat_address'],),
        abi=setup['vat_abi'],
        load_id=setup['load_id'],
        start_block=setup['start_block'] + 1,
        end_block=setup['end_block'],
        start_time=setup['start_time'],
        end_time=setup['end_time'],
    )

    print(f"""{len(records)} VAT operations prepared to write""")

    return records
