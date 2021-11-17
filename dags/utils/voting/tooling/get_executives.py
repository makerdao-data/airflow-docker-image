import requests
import json
from .get_executives_url import _get_executives_url


def get_execs():

    url = _get_executives_url()
    r = requests.get(url)
    content = json.loads(r.text)

    executives = content["pageProps"]["proposals"]

    return executives
