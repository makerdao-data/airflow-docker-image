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


def decode_logs(r):

    # kick: 0x7c5bfdc0a5e8192f6cd4972f382cec69116862fb62e6abff8003874c58e064b8
    # take: 0x05e309fd6ce72f2ab888a20056bb4210df08daed86f21f95053deb19964d86b1
    # redo: 0x275de7ecdd375b5e8049319f8b350686131c219dd4dc450a08e9cf83b03c865f

    kick = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "id", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "top", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "tab", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "lot", "type": "uint256"},
                {"indexed": True, "internalType": "address", "name": "usr", "type": "address"},
                {"indexed": True, "internalType": "address", "name": "kpr", "type": "address"},
                {"indexed": False, "internalType": "uint256", "name": "coin", "type": "uint256"},
            ],
            "name": "Kick",
            "type": "event",
        }
    ]

    take = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "id", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "max", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "price", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "owe", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "tab", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "lot", "type": "uint256"},
                {"indexed": True, "internalType": "address", "name": "usr", "type": "address"},
            ],
            "name": "Take",
            "type": "event",
        }
    ]

    redo = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "id", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "top", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "tab", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "lot", "type": "uint256"},
                {"indexed": True, "internalType": "address", "name": "usr", "type": "address"},
                {"indexed": True, "internalType": "address", "name": "kpr", "type": "address"},
                {"indexed": False, "internalType": "uint256", "name": "coin", "type": "uint256"},
            ],
            "name": "Redo",
            "type": "event",
        }
    ]

    events = []

    for i in r:

        if i[5][0] == '0x7c5bfdc0a5e8192f6cd4972f382cec69116862fb62e6abff8003874c58e064b8':
            topic_position = 1
            data_position = 2
            Kick = dict()
            for j in kick[0]['inputs']:
                if j['indexed'] == True:
                    if j['internalType'] == 'uint256':
                        Kick[j["name"]] = int(i[5][topic_position], 16)
                    elif j['internalType'] == 'address':
                        Kick[j["name"]] = '0x' + i[5][topic_position][-40:]
                    else:
                        pass
                    topic_position += 1
                else:
                    Kick[j["name"]] = int(i[4][data_position : data_position + 64], 16)
                    data_position += 64

            Kick['event'] = 'kick'
            Kick['tx_hash'] = i[1]
            Kick['block'] = i[7]

            print(Kick)
            events.append(Kick)

        elif i[5][0] == '0x05e309fd6ce72f2ab888a20056bb4210df08daed86f21f95053deb19964d86b1':

            topic_position = 1
            data_position = 2
            Take = dict()
            for j in take[0]['inputs']:
                if j['indexed'] == True:
                    if j['internalType'] == 'uint256':
                        Take[j["name"]] = int(i[5][topic_position], 16)
                    elif j['internalType'] == 'address':
                        Take[j["name"]] = '0x' + i[5][topic_position][-40:]
                    else:
                        pass
                    topic_position += 1
                else:
                    Take[j["name"]] = int(i[4][data_position : data_position + 64], 16)
                    data_position += 64

            Take['event'] = 'take'
            Take['tx_hash'] = i[1]
            Take['block'] = i[7]

            print(Take)
            events.append(Take)

        elif i[5][0] == '0x275de7ecdd375b5e8049319f8b350686131c219dd4dc450a08e9cf83b03c865f':

            topic_position = 1
            data_position = 2
            Redo = dict()
            for j in redo[0]['inputs']:
                if j['indexed'] == True:
                    if j['internalType'] == 'uint256':
                        Redo[j["name"]] = int(i[5][topic_position], 16)
                    elif j['internalType'] == 'address':
                        Redo[j["name"]] = '0x' + i[5][topic_position][-40:]
                    else:
                        pass
                    topic_position += 1
                else:
                    Redo[j["name"]] = int(i[4][data_position : data_position + 64], 16)
                    data_position += 64

            Redo['event'] = 'redo'
            Redo['tx_hash'] = i[1]
            Redo['block'] = i[7]

            print(Redo)
            events.append(Redo)

        else:

            pass

    return events
