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

import requests
import json
from web3 import Web3
from airflow.exceptions import AirflowFailException
import os, sys

sys.path.append('/opt/airflow/')
from dags.connectors.chain import chain
from dags.connectors.sf import sf


def get_changelog():

    r = requests.get('https://changelog.makerdao.com/releases/mainnet/active/contracts.json')
    if r.status_code == 200:
        changelog = json.loads(r.text)
    else:
        changelog = None

    return changelog


def get_clippers(DB=None):

    c = sf.execute(
        f"""select ilk
                    from {DB}.internal.clipper; """
    ).fetchall()

    clippers = []
    for i in c:
        clippers.append(i[0])

    return clippers


def update_clippers(DB=None, load_id=None):

    clippers = get_clippers(DB)

    clip_abi = [
        {
            "inputs": [],
            "name": "ilk",
            "outputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "calc",
            "outputs": [{"internalType": "contract AbacusLike", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    changelog = get_changelog()
    if changelog:
        for i in changelog:
            if '_CLIP_' in i and '_CALC_' not in i:
                Clip = chain.eth.contract(address=Web3.toChecksumAddress(changelog[i]), abi=clip_abi)
                ilk = Clip.functions.ilk().call().decode('utf-8').rstrip("\x00")
                clip = changelog[i]
                calc = Clip.functions.calc().call()

                if ilk not in clippers:

                    try:
                        sf.execute(
                            f"""INSERT INTO {DB}.internal.clipper(load_id, ilk, clip, calc)
                                        VALUES ('{load_id}', '{ilk}', '{clip.lower()}', '{calc.lower()}');"""
                        )
                    except Exception as e:
                        print(e)
                        raise AirflowFailException("#ERROR ON ADDING NEW CLIPPER")
    else:
        raise AirflowFailException("#ERROR: CHANGELOG UNAVAILABLE")

    return True
