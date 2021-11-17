from datetime import datetime
from operator import itemgetter
import json
from airflow.exceptions import AirflowFailException
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, sf_dict
from dags.adapters.contracts.tokens import balance_of
from dags.notifications.discord import notifier


def _votes(operations, new_polls, **setup):

    try:

        db_gov_operations = f"""
            SELECT
                o.order_index, o.block, o.timestamp,
                o.tx_hash, o.to_address, o.voter, o.amount,
                o.slate, o.yay1, o.yay2, o.yay3, o.yay4,
                o.yay5, o.function, o.option, o.proxy, o.cold,
                o.gas_used, o.gas_price
            FROM {setup['votes_db']}.internal.gov_operations o
            ORDER BY order_index; 
        """

        db_gov_rows = sf.execute(db_gov_operations).fetchall()

        fixed_rows = list()
        for i in operations:
            fixed_rows.append(
                [
                    i[0],
                    i[1],
                    datetime.strptime(i[2], '%Y-%m-%d %H:%M:%S'),
                    i[3],
                    i[4],
                    i[5],
                    i[6],
                    i[7],
                    i[9],
                    i[10],
                    i[11],
                    i[12],
                    i[13],
                    i[14],
                    i[8],
                    i[15],
                    i[16],
                    i[17],
                    i[18],
                ]
            )

        unordered_gov_operations = db_gov_rows + fixed_rows
        all_gov_operations = sorted(unordered_gov_operations, key=itemgetter(1))

        db_votes = f"""
            SELECT *
            FROM {setup['votes_db']}.public.votes
            ORDER BY order_index;
        """

        votes_rows = sf.execute(db_votes).fetchall()

        dsc1_1 = datetime.strptime(setup['dschief1_1_date'], "%Y-%m-%d %H:%M:%S")
        dsc1_2 = datetime.strptime(setup['dschief1_2_date'], "%Y-%m-%d %H:%M:%S")

        all_polls = dict()
        old_polls = sf.execute(
            f"""select code, end_timestamp from {setup['votes_db']}.internal.yays where type = 'poll'; """
        ).fetchall()
        for i in old_polls:
            if i[0] not in all_polls:
                all_polls.setdefault(i[0], i[1])

        for i in new_polls:
            if i[1] not in all_polls:
                all_polls.setdefault(i[1], i[3])

        query = f"""SELECT p.address, p.attributes
                FROM {setup['votes_db']}.internal.proxies p
                WHERE p.type = 'MCDVoteProxy'; """

        proxies = sf.execute(query)
        proxies = {p[0]: json.loads(p[1]) for p in proxies}

        start_block, start_time = sf.execute(
            f"""
            SELECT max(block), max(timestamp)
            FROM {setup['votes_db']}.public.votes; """
        ).fetchone()

        if not start_block:
            start_block, start_time = setup['start_block'], setup['start_time']

        voters = dict()
        yays = dict()
        hat = dict(address=None, approval=0, lift_approval=0, max_approval=0, voters=0)
        new_hat = None
        records = []

        for (
            order_index,
            block,
            timestamp,
            tx_hash,
            to_address,
            voter,
            amount,
            slate,
            yay1,
            yay2,
            yay3,
            yay4,
            yay5,
            function,
            option,
            proxy,
            cold,
            gas_used,
            gas_price,
        ) in all_gov_operations:

            # check and initialize the voters dictionary
            if voter not in voters:
                voters[voter] = dict(stakes=dict(), yays=[])

            # check and initialize the stakes dictionary
            if to_address not in voters[voter]['stakes']:
                voters[voter]['stakes'][to_address] = 0

            # lifting a hat and dropping previous one
            if function == 'create':

                if block > start_block:
                    r = [
                        setup['load_id'],
                        order_index,
                        block,
                        timestamp,
                        tx_hash,
                        None,
                        voter,
                        proxy,
                        0,
                        'CREATE_PROXY',
                        None,
                        None,
                        0,
                        False,
                        None,
                        gas_used,
                        gas_price,
                    ]
                    records.append(r)
                    votes_rows.append(r)

            elif function == 'break':

                if block > start_block:
                    r = [
                        setup['load_id'],
                        order_index,
                        block,
                        timestamp,
                        tx_hash,
                        None,
                        voter,
                        proxy,
                        0,
                        'BREAK_PROXY',
                        None,
                        None,
                        0,
                        False,
                        None,
                        gas_used,
                        gas_price,
                    ]
                    records.append(r)
                    votes_rows.append(r)

            elif function == 'lift':

                old_hat = hat['address']
                hat['address'] = new_hat = yay1
                hat['approval'] = hat['lift_approval'] = hat['max_approval'] = yays[yay1]['approval']
                hat['voters'] = len(yays[yay1]['voters'])

                if block > start_block:

                    if old_hat:
                        r = [
                            setup['load_id'],
                            order_index + '_0',
                            block,
                            timestamp,
                            tx_hash,
                            to_address,
                            voter,
                            None,
                            0,
                            'DROP',
                            old_hat,
                            None,
                            0,
                            False,
                            old_hat,
                            0,
                            0,
                        ]
                        records.append(r)
                        votes_rows.append(r)

                    r = [
                        setup['load_id'],
                        order_index + '_1',
                        block,
                        timestamp,
                        tx_hash,
                        to_address,
                        voter,
                        None,
                        0,
                        'LIFT',
                        hat['address'],
                        None,
                        0,
                        False,
                        hat['address'],
                        gas_used,
                        gas_price,
                    ]
                    records.append(r)
                    votes_rows.append(r)

            # stake operations
            elif function in ('lock', 'free'):

                # update the voter stake
                voters[voter]['stakes'][to_address] += amount

                if block > start_block:

                    r = [
                        setup['load_id'],
                        order_index + '_0',
                        block,
                        timestamp,
                        tx_hash,
                        to_address,
                        voter,
                        None,
                        round(amount, 6),
                        'STAKE' if function == 'lock' else 'WITHDRAW',
                        None,
                        None,
                        0,
                        False,
                        hat['address'],
                        gas_used,
                        gas_price,
                    ]
                    records.append(r)
                    votes_rows.append(r)

                # update voter's approvals
                for i, yay in enumerate(voters[voter]['yays']):

                    if yay and yay in yays:
                        yays[yay]['approval'] += amount

                    if new_hat and yays[yay]['approval'] > yays[new_hat]['approval']:
                        new_hat = yay
                        decisive = True
                    else:
                        decisive = False

                    if block > start_block:
                        r = [
                            setup['load_id'],
                            order_index + '_%d' % (i + 1),
                            block,
                            timestamp,
                            tx_hash,
                            to_address,
                            voter,
                            None,
                            0,
                            'UP-VOTE' if function == 'lock' else 'DOWN-VOTE',
                            yay,
                            None,
                            amount,
                            decisive,
                            hat['address'],
                            0,
                            0,
                        ]
                        records.append(r)
                        votes_rows.append(r)

            # vote operations
            elif function == 'vote':

                sum_yays = 0
                for y in (yay1, yay2, yay3, yay4, yay5):
                    if y:
                        sum_yays += 1

                counter = 0
                # remove previous approvals
                for yay in voters[voter]['yays']:

                    if yay and yay in yays:
                        yays[yay]['approval'] -= voters[voter]['stakes'][to_address]

                        if block > start_block:

                            r = [
                                setup['load_id'],
                                order_index + '_%d' % counter,
                                block,
                                timestamp,
                                tx_hash,
                                to_address,
                                voter,
                                None,
                                0,
                                'MOVE VOTE',
                                yay,
                                None,
                                -1 * voters[voter]['stakes'][to_address],
                                False,
                                hat['address'],
                                0,
                                0,
                            ]
                            records.append(r)
                            votes_rows.append(r)
                            counter += 1

                voters[voter]['yays'] = []

                # add new approvals
                for yay in (yay1, yay2, yay3, yay4, yay5):

                    if yay:

                        decisive = False

                        # check and initialize the yay stake
                        if yay not in yays:
                            yays[yay] = dict(approval=0, since=timestamp, last_voting=timestamp, voters=set())

                        yays[yay]['approval'] += voters[voter]['stakes'][to_address]
                        yays[yay]['last_voting'] = timestamp
                        yays[yay]['voters'].add(voter)
                        voters[voter]['yays'].append(yay)

                        if yay == hat['address']:
                            hat['last_voting'] = yays[yay]['last_voting']
                            hat['approval'] = yays[yay1]['approval']
                            hat['max_approval'] = max(hat['max_approval'], yays[yay1]['approval'])
                            hat['voters'] = len(yays[yay]['voters'])

                        if new_hat and yays[yay]['approval'] > yays[new_hat]['approval']:
                            new_hat = yay
                            decisive = True

                        if block > start_block:
                            r = [
                                setup['load_id'],
                                order_index + '_%d' % counter,
                                block,
                                timestamp,
                                tx_hash,
                                to_address,
                                voter,
                                None,
                                0,
                                'VOTE',
                                yay,
                                None,
                                voters[voter]['stakes'][to_address],
                                decisive,
                                hat['address'],
                                gas_used / sum_yays,
                                gas_price,
                            ]
                            records.append(r)
                            votes_rows.append(r)
                            counter += 1

            # polls operations - only for new records
            elif function == 'choose' and block > start_block:

                if slate and option:

                    poll = slate

                    options_hex = hex(int(option))[2:]
                    options = []
                    while len(options_hex):
                        option_hex = options_hex[-2:]
                        options.append(str(int(option_hex, 16)))
                        options_hex = options_hex[:-2]

                    # string containing ordered list of selected options
                    options_list = ','.join(options)

                    voting_power = 0

                    # # the voting power is the amount of MKRs in the wallet
                    # initial_voting_power = balance_of(setup['mkr_address'], voter, block)

                    # ... plus the current stake from Chiefs
                    if voter in voters:
                        for chief_address in setup['chief_addresses']:
                            voting_power += voters[voter]['stakes'].get(chief_address, 0)

                    # if proxy:

                    #     # ... plus MKRs amount in vote proxy  wallet
                    #     initial_voting_power += balance_of(setup['mkr_address'], proxy, block)

                    #     # ... plus MKRs amount in cold wallet
                    #     if cold and cold.lower() != voter.lower():
                    #         initial_voting_power += balance_of(setup['mkr_address'], cold, block)

                    # # it should be updated with every data load until poll is closed
                    # current_voting_power = initial_voting_power

                    prev_choose = None
                    for i in votes_rows:

                        if i[4] == tx_hash and i[6] == voter:
                            break

                        if i[10] == poll and i[6] == voter and i[4] != tx_hash:

                            prev_choose = dict()
                            prev_choose['voting_power'] = i[12]
                            prev_choose['option'] = i[11]

                    if not prev_choose:

                        r = [
                            setup['load_id'],
                            order_index + '_0',
                            block,
                            timestamp,
                            tx_hash,
                            to_address,
                            voter,
                            proxy,
                            0,
                            'CHOOSE',
                            poll,
                            options_list,
                            voting_power,
                            False,
                            hat['address'],
                            gas_used,
                            gas_price,
                        ]
                        records.append(r)
                        votes_rows.append(r)

                    else:

                        r = [
                            setup['load_id'],
                            order_index + '_0',
                            block,
                            timestamp,
                            tx_hash,
                            to_address,
                            voter,
                            proxy,
                            0,
                            'RETRACT',
                            poll,
                            prev_choose['option'],
                            prev_choose['voting_power'] * -1,
                            False,
                            hat['address'],
                            0,
                            0,
                        ]

                        records.append(r)
                        votes_rows.append(r)

                        r = [
                            setup['load_id'],
                            order_index + '_1',
                            block,
                            timestamp,
                            tx_hash,
                            to_address,
                            voter,
                            proxy,
                            0,
                            'CHOOSE',
                            poll,
                            options_list,
                            voting_power,
                            False,
                            hat['address'],
                            gas_used,
                            gas_price,
                        ]
                        records.append(r)
                        votes_rows.append(r)

        output = list()
        for i in records:
            output.append(i[:3] + [datetime.strftime(i[3], "%Y-%m-%d %H:%M:%S")] + i[4:])

    except Exception as e:
        print(e)
        raise AirflowFailException("FATAL: Error on creating votes records.")

    return output
