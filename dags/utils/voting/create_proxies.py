from dags.connectors.gcp import bq_query


def _create_proxies(**setup):

    proxies = list()
    query = f"""
        select l.block_number, l.transaction_index, l.log_index, l.block_timestamp,
            l.transaction_hash, l.topics, l.address, tr.gas_used, tx.gas_price
        from `bigquery-public-data.crypto_ethereum.logs` l
        JOIN `bigquery-public-data.crypto_ethereum.traces` tr
        ON l.transaction_hash = tr.transaction_hash
        JOIN `bigquery-public-data.crypto_ethereum.transactions` tx
        ON l.transaction_hash = tx.`hash`
        where l.topics[safe_offset(0)] = '{setup['create_proxy_topic']}'
            and (l.address = '{setup['vote_proxy_factory']}' or l.address = '{setup['old_vote_proxy_factory']}')
            and array_length(l.topics) = 4
            and l.block_timestamp >= '{setup['start_time']}'
            and l.block_timestamp <= '{setup['end_time']}'
            and tr.block_timestamp >= '{setup['start_time']}'
            and tr.block_timestamp <= '{setup['end_time']}'
            and tr.to_address in ('{setup['vote_proxy_factory']}', '{setup['old_vote_proxy_factory']}')
            and substr(l.topics[safe_offset(2)], -40) = substr(tr.from_address, -40)
            and tx.block_timestamp >= '{setup['start_time']}'
            and tx.block_timestamp <= '{setup['end_time']}'
        order by l.block_timestamp;
    """

    events = bq_query(query)
    for block, tx_index, log_index, timestamp, tx_hash, topics, address, gas_used, gas_price in events:

        cold = '0x' + topics[1][26:]
        hot = '0x' + topics[2][26:]
        proxy = '0x' + topics[3][26:]
        proxies.append(
            [
                setup['load_id'],
                block,
                tx_index,
                timestamp.__str__()[:19],
                tx_hash,
                cold.lower(),
                hot.lower(),
                proxy.lower(),
                'create',
                log_index,
                hot.lower(),
                address.lower(),
                gas_used,
                gas_price,
            ]
        )

    return proxies
