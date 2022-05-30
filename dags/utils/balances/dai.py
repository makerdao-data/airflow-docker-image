from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException
import sys
sys.path.append('/opt/airflow/')
from dags.connectors.sf import sf
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage


# get balances
def get_dai_balances(date):

    query = f"""
        SELECT address, raw_balance
        FROM maker.balances.dai
        WHERE date = '{date}';
        """

    sf.execute(query)
    records = sf.fetchall()

    balances = dict()
    if records != []:
        for i in records:
            balances[i[0]] = i[1]

    return balances


def get_dai_transfers(date):

    query = f"""
        SELECT timestamp, sender, receiver, raw_amount
        FROM MAKER.TRANSFERS.DAI
        WHERE DATE(timestamp) = '{date}'
        ORDER BY block, order_id;
        """

    sf.execute(query)
    records = sf.fetchall()

    return records


def update_balances(balances, transfers):

    for timestamp, sender, receiver, amount in transfers:

        if sender[:3] == '0xx':
            sender = '0x0' + sender[3:]
        if receiver[:3] == '0xx':
            receiver = '0x0' + receiver[3:]

        if amount > 0:

            # from_
            if sender not in balances:
                if sender != '0x0000000000000000000000000000000000000000':
                    balances[sender] = amount * -1

            else:

                current_address_balance = balances[sender]
                updated_address_balance = current_address_balance + amount * -1
                # remove 0 balance address
                if updated_address_balance == 0:
                    del balances[sender]
                else:
                    balances[sender] = updated_address_balance

            # to_
            if receiver != '0x0000000000000000000000000000000000000000':
                if receiver not in balances:
                    balances[receiver] = amount

                else:

                    current_address_balance = balances[receiver]
                    updated_address_balance = current_address_balance + amount
                    # remove 0 balance address
                    if updated_address_balance == 0:
                        del balances[receiver]
                    else:
                        balances[receiver] = updated_address_balance

    return balances


def save_balances(balances, date):

    load_id = datetime.now().__str__()[:19]
    records = []
    for i in balances.items():
        records.append([load_id, date, i[0], i[1], i[1] / 10**18])

    pattern = None
    if records:
        pattern = _write_to_stage(sf, records, f"MAKER.BALANCES.BALANCES_STORAGE")

        _write_to_table(
            sf,
            f"MAKER.BALANCES.BALANCES_STORAGE",
            f"MAKER.BALANCES.DAI",
            pattern,
        )
        _clear_stage(sf, f"MAKER.BALANCES.BALANCES_STORAGE", pattern)

    return


def processing(last_day, next_day):
    # get the balances from the last day available in balances table
    balances = get_dai_balances(last_day)
    # get transfers from next day after last day available in balances table
    transfers = get_dai_transfers(next_day)
    # update balances with transfers from next day after last day available in balances table
    balances = update_balances(balances, transfers)
    # save updated balances to db
    save_balances(balances, next_day)

    return


def update_dai_balances():
    last_day = sf.execute("""
        select max(date)
        from maker.balances.dai;
    """).fetchone()[0]

    if not last_day:
        last_day = datetime(2019, 11, 12).date()

    yesterday = (datetime.utcnow() - timedelta(days=1)).date()

    days_to_compute = []
    while last_day < yesterday:

        last_day = last_day + timedelta(days=1)
        days_to_compute.append(last_day)

    for day in days_to_compute:
        # compute
        processing(day -timedelta(days=1), day)