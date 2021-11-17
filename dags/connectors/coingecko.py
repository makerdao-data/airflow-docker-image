from datetime import datetime


def get_gecko_price(tst, token, gecko_prices):

    if token in gecko_prices:

        for i in gecko_prices[token]:
            price = i[1]
            if int(i[0] / 1000) > tst:
                break

    return price
