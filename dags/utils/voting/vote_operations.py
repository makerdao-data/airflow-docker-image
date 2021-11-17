from dags.utils.voting.tooling.current_proxy import _current_proxy


def _vote_operations(chief, polls, lastest_proxies_history, full_proxies_history, **setup):

    vote_operations = list()

    etch_vote_new = dict()
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
        gas_price,
    ) in chief:
        if function in ('etch', 'vote_new') and status == 1:
            args = arguments[0]['value']
            while len(args) < 5:
                args.append(None)
            out = outputs[0]['value']
            r = args + [out] + [None]
            etch_vote_new[out] = r

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
    ) in lastest_proxies_history:

        r = [
            str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + str(breadcrumb).zfill(3),
            block,
            timestamp,
            tx_hash,
            to_address,
            from_address,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            action,
            proxy,
            cold,
            gas_used,
            gas_price,
        ]

        vote_operations.append(r)

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
        gas_price,
    ) in chief:

        proxy = None
        cold = None
        proxy_bundle = _current_proxy(full_proxies_history, timestamp, from_address)
        if proxy_bundle:
            from_address = proxy_bundle[from_address]['hot']
            proxy = proxy_bundle[from_address]['proxy']
            cold = proxy_bundle[from_address]['cold']

        if status == 1:
            if function == 'lock':

                r = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    to_address,
                    from_address,
                    int(arguments[0]['value']) / 10 ** 18,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    function,
                    proxy,
                    cold,
                    gas_used,
                    gas_price,
                ]

                vote_operations.append(r)

            elif function == 'free':

                r = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    to_address,
                    from_address,
                    (int(arguments[0]['value']) / 10 ** 18) * -1,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    function,
                    proxy,
                    cold,
                    gas_used,
                    gas_price,
                ]

                vote_operations.append(r)

            elif function == 'vote':

                if arguments[0]['name'] == 'slate' and arguments[0]['value'] in etch_vote_new:

                    j = etch_vote_new[arguments[0]['value']]
                    r = [
                        str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                        block,
                        timestamp,
                        tx_hash,
                        to_address,
                        from_address,
                        0,
                        j[5],
                        None,
                        j[0],
                        j[1],
                        j[2],
                        j[3],
                        j[4],
                        function,
                        proxy,
                        cold,
                        gas_used,
                        gas_price,
                    ]

                    vote_operations.append(r)

            elif function == 'vote_new':

                args = arguments[0]['value']
                while len(args) < 5:
                    args.append(None)

                r = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    to_address,
                    from_address,
                    0,
                    str(outputs[0]['value']),
                    None,
                    args[0],
                    args[1],
                    args[2],
                    args[3],
                    args[4],
                    'vote',
                    proxy,
                    cold,
                    gas_used,
                    gas_price,
                ]

                vote_operations.append(r)

            elif function == 'lift':

                r = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    to_address,
                    from_address,
                    0,
                    None,
                    None,
                    str(arguments[0]['value']),
                    None,
                    None,
                    None,
                    None,
                    'lift',
                    proxy,
                    cold,
                    gas_used,
                    gas_price,
                ]

                vote_operations.append(r)

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
        gas_price,
    ) in polls:
        if status == 1:
            if function == 'vote':

                proxy = None
                cold = None
                proxy_bundle = _current_proxy(full_proxies_history, timestamp, from_address)
                if proxy_bundle:
                    from_address = proxy_bundle[from_address]['hot']
                    proxy = proxy_bundle[from_address]['proxy']
                    cold = proxy_bundle[from_address]['cold']

                args = arguments
                r = [
                    str(block).zfill(9) + '_' + str(tx_index).zfill(3) + '_' + breadcrumb,
                    block,
                    timestamp,
                    tx_hash,
                    to_address,
                    from_address,
                    0,
                    str(args[0]['value']),
                    str(args[1]['value']),
                    None,
                    None,
                    None,
                    None,
                    None,
                    'choose',
                    proxy,
                    cold,
                    gas_used,
                    gas_price,
                ]

                vote_operations.append(r)

    return vote_operations
