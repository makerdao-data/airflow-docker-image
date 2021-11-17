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

# Data Services ETL connector for Coinpaprika API

from decimal import Decimal
from datetime import timedelta, timezone
from dateutil.parser import parse
import time

from coinpaprika import client
from coinpaprika.exceptions import CoinpaprikaAPIException

# global storage of price history
price_ticks = dict()


def round_minutes(dt, direction, resolution):

    new_minute = (dt.minute // resolution + (1 if direction == 'up' else 0)) * resolution
    _dt = dt + timedelta(minutes=new_minute - dt.minute)

    return _dt.replace(second=0, microsecond=0, tzinfo=timezone.utc)


def get_cp_coins():

    coins = coinpaprika.coins()
    cp_coins = [coin['id'] for coin in coins]

    return cp_coins


def get_cp_price(timestamp, gem):

    if gem['cp_id'] not in cp_coins:
        return None

    timestamp = round_minutes(timestamp, 'up', 5)

    if (timestamp, gem['address']) not in price_ticks:
        timestamp_str = timestamp.__str__()[:10] + 'T' + timestamp.__str__()[11:19] + 'Z'
        while True:
            try:
                _prices = coinpaprika.historical(gem['cp_id'], start=timestamp_str, limit=1000)
                if len(_prices):
                    row = _prices[0]
                    while timestamp < parse(row['timestamp']):
                        price_ticks[(timestamp, gem['address'])] = None
                        timestamp += timedelta(minutes=5)
                    for row in _prices:
                        price_ticks[(parse(row['timestamp']), gem['address'])] = round(
                            Decimal(row['price']), 4
                        )
                else:
                    price_ticks[(timestamp, gem['address'])] = None
                break
            except CoinpaprikaAPIException as e:
                if e.status_code != 429:
                    price_ticks[(timestamp, gem['address'])] = None
                    break
                else:

                    print()
                    print()
                    print(e)
                    print("RATE LIMIT HIT:: get_cp_price")
                    print()
                    print()

                    time.sleep(1)

            except Exception as e:

                print()
                print()
                print(e)
                print("UNKNOWN ERROR:: get_cp_price")
                print()
                print()

                time.sleep(1)

    return price_ticks.get((timestamp, gem['address']))


def get_current_cp_price(gem):

    if gem['cp_id'] not in cp_coins:
        return None

    _price = None

    while True:
        try:
            _price = coinpaprika.ticker(gem['cp_id'])['quotes']['USD']['price']
            break
        except CoinpaprikaAPIException as e:
            if e.status_code != 429:
                break
            else:
                time.sleep(1)
        except:
            time.sleep(1)

    return _price


# coinpaprika = client.Client()
# cp_coins = get_cp_coins()
