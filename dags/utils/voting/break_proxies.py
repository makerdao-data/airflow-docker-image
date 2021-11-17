from dags.connectors.gcp import bq_query


def _break_proxies(**setup):

    query = f"""
    select tr.block_number, tr.transaction_index, tr.block_timestamp, tr.transaction_hash,
        tr.to_address, tr.input, concat('0x', right(tr.output, 40)) wallet,
        (SELECT STRING_AGG(FORMAT('%03d', CAST(IF(traces='', '0', traces) AS INT64)), '_') FROM UNNEST(SPLIT(tr.trace_address)) AS traces) as breadcrumb,
        tr.from_address, tr.gas_used, tx.gas_price
    from `bigquery-public-data.crypto_ethereum.traces` tr
    JOIN `bigquery-public-data.crypto_ethereum.transactions` tx
    ON tr.transaction_hash = tx.`hash`
    where tr.input in ('0x578e9dc5', '0xdde9c297')
        and tr.from_address in (lower('{setup['vote_proxy_factory']}'), lower('{setup['old_vote_proxy_factory']}'))
        and tr.block_timestamp >= '{setup['start_time']}'
        and tr.block_timestamp <= '{setup['end_time']}'
        and tr.status = 1
        and tx.block_timestamp >= '{setup['start_time']}'
        and tx.block_timestamp <= '{setup['end_time']}'
        and tx.to_address in (lower('{setup['vote_proxy_factory']}'), lower('{setup['old_vote_proxy_factory']}'))
    order by tr.block_number, tr.transaction_index;
    """

    actions = bq_query(query)
    brakes = dict()
    for (
        block,
        tx_index,
        timestamp,
        tx_hash,
        proxy,
        function,
        wallet,
        breadcrumb,
        from_address,
        gas_used,
        gas_price,
    ) in actions:

        brakes.setdefault(tx_hash, {})
        brakes[tx_hash].setdefault('tx_index', tx_index)
        brakes[tx_hash].setdefault('block', block)
        brakes[tx_hash].setdefault('timestamp', timestamp)
        brakes[tx_hash].setdefault('tx_hash', tx_hash)
        brakes[tx_hash].setdefault('proxy', proxy),
        if function == '0x578e9dc5':
            brakes[tx_hash].setdefault('cold', wallet)
        elif function == '0xdde9c297':
            brakes[tx_hash].setdefault('hot', wallet)
        else:
            brakes[tx_hash].setdefault('cold', None)
            brakes[tx_hash].setdefault('hot', None)
        brakes[tx_hash].setdefault('breadcrumb', breadcrumb)
        brakes[tx_hash].setdefault('from_address', from_address)
        brakes[tx_hash].setdefault('to_address', proxy)
        brakes[tx_hash].setdefault('gas_used', gas_used)
        brakes[tx_hash].setdefault('gas_price', gas_price)

    break_proxies = list()
    for tx_hash in brakes:

        break_proxies.append(
            [
                setup['load_id'],
                brakes[tx_hash]['block'],
                brakes[tx_hash]['tx_index'],
                brakes[tx_hash]['timestamp'].__str__()[:19],
                tx_hash,
                brakes[tx_hash]['cold'].lower(),
                brakes[tx_hash]['hot'].lower(),
                brakes[tx_hash]['proxy'].lower(),
                'break',
                brakes[tx_hash]['breadcrumb'],
                brakes[tx_hash]['from_address'],
                brakes[tx_hash]['to_address'],
                brakes[tx_hash]['gas_used'],
                brakes[tx_hash]['gas_price'],
            ]
        )

    return break_proxies
