from datetime import datetime


def _current_proxy(history, end_timestamp, voter):

    proxy_bundle = dict()
    for (
        load_id,
        block,
        tx_index,
        timestamp,
        tx_hash,
        cold,
        hot,
        proxy,
        action,
        breadcrumb,
        from_address,
        to_address,
        gas_used,
        gas_price,
    ) in history:

        if datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') <= datetime.strptime(
            end_timestamp, '%Y-%m-%d %H:%M:%S'
        ) and voter.lower() in [proxy.lower(), hot.lower()]:

            if action == 'create':
                proxy_bundle[voter] = dict(cold=cold, proxy=proxy, hot=hot)
                proxy_bundle[hot] = dict(cold=cold, proxy=proxy, hot=hot)
                proxy_bundle[proxy] = dict(cold=cold, proxy=proxy, hot=hot)
            elif action == 'break':
                try:
                    if voter in proxy_bundle:
                        proxy_bundle.pop(voter)
                    if hot in proxy_bundle:
                        proxy_bundle.pop(hot)
                    if proxy in proxy_bundle:
                        proxy_bundle.pop(proxy)
                except Exception as e:
                    print(e)
                    print(voter)
            else:
                pass

    return proxy_bundle
