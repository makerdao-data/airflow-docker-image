#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from datetime import datetime
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf, sf_dict


def update_round_num(sf, id, ilk, round_num, DB):

    q = f"""UPDATE {DB}.internal.action
            SET round = {round_num}
            WHERE id = {id}
                AND ilk = '{ilk}'; """

    x = sf.execute(q)

    return x


def rounds_auctions(load_id, start_block, end_block, start_time, end_time, DB, STAGING):

    initial = sf.execute(
        f"""select id, block
                            from {DB}.internal.action
                            where
                                round is null
                                and status = 1
                            order by block, id
                            limit 1; """
    ).fetchone()

    if initial:

        actions = sf_dict.execute(
            f"""select *
                                    from {DB}.internal.action
                                    where
                                        type in ('kick', 'redo', 'take')
                                        and status = 1
                                        and id >= {initial[0]}
                                        and block >= {initial[1]}
                                    order by block, id; """
        ).fetchall()

        if actions:
            for a in actions:

                # kick
                if a['TYPE'] == 'kick':

                    q = f"""select *
                        from {DB}.internal.bark
                        where auction_id = {a['AUCTION_ID']}
                        and ilk = '{a['ILK']}'; """

                    bark = sf_dict.execute(q).fetchone()

                    # create AUCTION
                    action_start = datetime.strftime(a['TIMESTAMP'], '%Y-%m-%d %H:%S:%M')
                    round_num = 1

                    sf.execute(
                        f"""INSERT INTO {DB}.internal.auction(load_id, auction_id, auction_start, vault,
                                                                    ilk, urn, owner, debt, available_collateral,
                                                                    penalty, round, finished)
                                    VALUES ('{load_id}', {a['AUCTION_ID']}, '{action_start}', '{bark['VAULT']}',
                                            '{bark['ILK']}', '{a['URN']}', '{bark['OWNER']}', {a['DEBT']},
                                            {a['AVAILABLE_COLLATERAL']}, {bark['PENALTY']}, {round_num}, 0); """.replace(
                            'None', 'NULL'
                        )
                    )

                    # create #1 ROUND
                    sf.execute(
                        f"""INSERT INTO {DB}.internal.round(load_id, auction_id, round, round_start, initial_price,
                                                                    debt, available_collateral, keeper, incentives, finished, ilk)
                                    VALUES ('{load_id}', {a['AUCTION_ID']}, {round_num}, '{a['TIMESTAMP']}', {a['INIT_PRICE']},
                                            {a['DEBT']}, {a['AVAILABLE_COLLATERAL']}, '{a['KEEPER']}', '{a['INCENTIVES']}', 0, '{a['ILK']}'); """
                    )

                    # update liq_actions with round_num
                    update_round_num(sf, a['ID'], a['ILK'], round_num, DB)

                # take
                elif a['TYPE'] == 'take':

                    round_num = sf.execute(
                        f"""SELECT max(round)
                                                FROM {DB}.internal.round
                                                WHERE auction_id = {a['AUCTION_ID']}
                                                    AND ilk = '{a['ILK']}'; """
                    ).fetchone()[0]

                    q = f"""SELECT sold_collateral, recovered_debt
                            FROM {DB}.internal.round
                            WHERE auction_id = {a['AUCTION_ID']} AND
                                ilk = '{a['ILK']}' AND
                                round = {round_num}; """

                    record = sf.execute(q).fetchone()

                    sold_collateral = (record[0] if record[0] else 0) + (
                        a['SOLD_COLLATERAL'] if a['SOLD_COLLATERAL'] else 0
                    )
                    recovered_debt = (record[1] if record[1] else 0) + (
                        a['RECOVERED_DEBT'] if a['RECOVERED_DEBT'] else 0
                    )

                    if a['CLOSING_TAKE'] == 1:

                        sf.execute(
                            f"""UPDATE {DB}.internal.auction
                                        SET finished = {int(a['CLOSING_TAKE'])},
                                            auction_end = '{a['TIMESTAMP']}',
                                            available_collateral = {a['AVAILABLE_COLLATERAL']},
                                            debt = {a['DEBT']},
                                            sold_collateral = {sold_collateral},
                                            recovered_debt = {recovered_debt}
                                        WHERE auction_id = {a['AUCTION_ID']} AND
                                            ilk = '{a['ILK']}'; """
                        )

                    else:

                        q = f"""UPDATE {DB}.internal.auction
                                        SET finished = {int(a['CLOSING_TAKE'])},
                                            available_collateral = {a['AVAILABLE_COLLATERAL']},
                                            debt = {a['DEBT']},
                                            sold_collateral = {sold_collateral},
                                            recovered_debt = {recovered_debt}
                                        WHERE auction_id = {a['AUCTION_ID']} AND
                                            ilk = '{a['ILK']}'; """

                        sf.execute(q)

                    if a['CLOSING_TAKE'] == 1:

                        sf.execute(
                            f"""UPDATE {DB}.internal.round
                                        SET finished = {int(a['CLOSING_TAKE'])},
                                            round_end = '{a['TIMESTAMP']}',
                                            end_price = {a['COLLATERAL_PRICE']},
                                            available_collateral = {a['AVAILABLE_COLLATERAL']},
                                            debt = {a['DEBT']},
                                            sold_collateral = {sold_collateral},
                                            recovered_debt = {recovered_debt}
                                        WHERE auction_id = {a['AUCTION_ID']} AND
                                            ilk = '{a['ILK']}' AND
                                            round = {round_num}; """
                        )

                    else:

                        sf.execute(
                            f"""UPDATE {DB}.internal.round
                                        SET finished = {int(a['CLOSING_TAKE'])},
                                            end_price = {a['COLLATERAL_PRICE']},
                                            available_collateral = {a['AVAILABLE_COLLATERAL']},
                                            debt = {a['DEBT']},
                                            sold_collateral = {sold_collateral},
                                            recovered_debt = {recovered_debt}
                                        WHERE auction_id = {a['AUCTION_ID']} AND
                                            ilk = '{a['ILK']}' AND
                                            round = {round_num}; """
                        )

                    # update liq_actions with round_num
                    update_round_num(sf, a['ID'], a['ILK'], round_num, DB)

                elif a['TYPE'] == 'redo':

                    last_round = sf.execute(
                        f"""select max(round)
                                    from {DB}.internal.round
                                    where auction_id = {a['AUCTION_ID']}
                                        and ilk = '{a['ILK']}'; """
                    ).fetchone()[0]

                    round_num = 1
                    if last_round:
                        round_num = last_round + 1

                    # create NEW ROUND
                    sf.execute(
                        f"""INSERT INTO {DB}.internal.round (load_id, auction_id, round, round_start, initial_price,
                                                                    debt, available_collateral, keeper, incentives, finished, ilk)
                                    VALUES ('{load_id}', {a['AUCTION_ID']}, {round_num}, '{a['TIMESTAMP']}', {a['INIT_PRICE']},
                                            {a['DEBT']}, {a['AVAILABLE_COLLATERAL']}, '{a['KEEPER']}', '{a['INCENTIVES']}', 0, '{a['ILK']}'); """
                    )

                    # update liq_actions with round_num
                    update_round_num(sf, a['ID'], a['ILK'], round_num, DB)

                else:

                    pass

        print(f'Parsed {len(actions)} actions.')

    else:

        print('Nothing to do.')

    return True
