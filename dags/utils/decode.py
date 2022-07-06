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


class Block:
    def __init__(self, raw_block):

        self.block_number = raw_block.number
        self.timestamp = raw_block.timestamp
        self.block_hash = raw_block.hash
        self.miner = raw_block.miner
        self.difficulty = raw_block.difficulty
        self.size = raw_block.size
        self.extra_data = raw_block.extra_data
        self.gas_limit = raw_block.gas_limit
        self.gas_used = raw_block.gas_used
        self.tx_count = raw_block.transaction_count


class Transfer:
    def __init__(self, raw_transfer):

        self.token_address = raw_transfer.token_address
        self.from_address = raw_transfer.from_address
        self.to_address = raw_transfer.to_address
        self.value = raw_transfer.value
        self.hash = raw_transfer.hash
        self.sender = raw_transfer.sender
        self.receiver = raw_transfer.receiver

        self.timestamp = raw_transfer.block_timestamp
        self.block_number = raw_transfer.block_number


class Call:
    def __init__(self, raw_trace, abi):

        self.tx_hash = raw_trace.transaction_hash
        self.tx_index = raw_trace.transaction_index
        self.breadcrumb = raw_trace.breadcrumb or '000'
        self.type = raw_trace.trace_type
        self.from_address = raw_trace.from_address
        self.to_address = raw_trace.to_address
        self.value = raw_trace.value

        self.function, self.arguments, self.outputs = decode_call(raw_trace.input, raw_trace.output, abi)

        self.gas_used = raw_trace.gas_used or 0
        self.error = raw_trace.error
        self.timestamp = raw_trace.block_timestamp
        self.block_number = raw_trace.block_number
        if raw_trace.status == 0 or (self.error is not None and len(self.error) > 0):
            self.status = 0
        else:
            self.status = 1


class EDWCall:
    def __init__(self, raw_trace, abi):

        self.tx_hash = raw_trace[0]
        self.tx_index = raw_trace[1]
        self.breadcrumb = raw_trace[2] or '000'
        self.type = 'call'
        self.from_address = raw_trace[3]
        self.to_address = raw_trace[4]
        self.value = raw_trace[5]

        self.function, self.arguments, self.outputs = decode_call(raw_trace[6], raw_trace[7], abi)

        self.gas_used = raw_trace[8] or 0
        self.error = raw_trace[9]
        self.timestamp = raw_trace[11]
        self.block_number = raw_trace[12]
        if raw_trace[10] == 0 or (self.error is not None and len(self.error) > 0):
            self.status = 0
        else:
            self.status = 1


class GasCall:
    def __init__(self, raw_trace, abi):

        self.tx_hash = raw_trace.transaction_hash
        self.tx_index = raw_trace.transaction_index
        self.breadcrumb = raw_trace.breadcrumb or '000'
        self.type = raw_trace.trace_type
        self.from_address = raw_trace.from_address
        self.to_address = raw_trace.to_address
        self.value = raw_trace.value

        self.function, self.arguments, self.outputs = decode_call(raw_trace.input, raw_trace.output, abi)

        self.gas_used = raw_trace.gas_used or 0
        self.error = raw_trace.error
        self.timestamp = raw_trace.block_timestamp
        self.block_number = raw_trace.block_number
        if raw_trace.status == 0 or (self.error is not None and len(self.error) > 0):
            self.status = 0
        else:
            self.status = 1
        self.gas_price = raw_trace.gas_price or 0


def _decode_static_argument(raw_value, argument_type):

    decoded_value = raw_value

    if decoded_value:
        if argument_type == 'address':
            if len(raw_value) >= 40:
                decoded_value = '0x' + raw_value[-40:]
            else:
                decoded_value = raw_value
        elif argument_type[:4] == 'uint':
            if type(raw_value) is str:
                decoded_value = int(raw_value, 16)
            else:
                decoded_value = raw_value
        elif argument_type[:3] == 'int':
            if type(raw_value) is str:
                decoded_value = int(raw_value, 16)
                if decoded_value & (1 << (256 - 1)):
                    decoded_value -= 1 << 256
            else:
                decoded_value = raw_value
        elif argument_type == 'bool':
            if int(raw_value, 16) == 0:
                decoded_value = "False"
            else:
                decoded_value = "True"
        elif argument_type == "bytes":
            decoded_value = "0x" + bytes.fromhex(raw_value[2:])[: int(argument_type[5:])].hex()

        elif argument_type == "byte":
            decoded_value = "0x" + bytes.fromhex(raw_value[2:])[0].hex()

        elif argument_type in ("string", "bstring"):
            try:
                if raw_value[:2] == "0x":
                    raw_value = raw_value[2:]
                decoded_value = bytes.fromhex(raw_value).decode('utf-8').replace('\x00', '')
            except:
                pass

        elif argument_type == 'timestamp':
            if type(raw_value) is str:
                decoded_value = str(datetime.fromtimestamp(int(raw_value, 16)))
            else:
                decoded_value = str(datetime.fromtimestamp(raw_value))
        elif argument_type == 'hashmap':
            decoded_value = "[...]"
        elif argument_type == 'tuple':
            decoded_value = "(...)"
        elif argument_type == 'tuple[]':
            decoded_value = "(...)[]"

    return decoded_value


def _decode_dynamic_array(data, array_type):
    count = int(data[:64], 16)
    sub_bytes = data[64:]
    decoded_argument = []

    for c in range(count):
        decoded = _decode_static_argument(sub_bytes[:64], array_type)
        decoded_argument.append(decoded)
        sub_bytes = sub_bytes[64:]

    return decoded_argument


def _decode_dynamic_argument(argument_bytes, argument_type):

    if len(argument_bytes):
        length = int(argument_bytes[:64], 16) * 2
        value = argument_bytes[64 : 64 + length]

        if argument_type == "string":
            decoded_value = bytes.fromhex(value).decode('utf-8').replace('\x00', '')
        else:
            decoded_value = value
    else:
        decoded_value = bytes(0)

    return decoded_value


def _decode_tuple(data, argument_abi, is_list):

    slots = 0

    if is_list:

        count = int(data[:64], 16)
        data = data[64:]
        decoded_argument = []

        for c in range(count):

            do_offset = any(a["dynamic"] for a in argument_abi)
            if do_offset:
                raw_value = data[c * 64 : (c + 1) * 64]
                offset = int(raw_value, 16) * 2
                sub_bytes = data[offset:]
            else:
                sub_bytes = data

            decoded, num = _decode_struct(sub_bytes, argument_abi)
            decoded_argument.append(decoded)
            slots += num

    else:
        decoded_argument, num = _decode_struct(data, argument_abi)
        slots += num

    return decoded_argument, slots


def _decode_struct(data, arguments_abi):
    if arguments_abi:
        no_arguments = len(arguments_abi)
    else:
        no_arguments = len(data) // 64 + 1

    arguments_list = []
    slot = 0
    for i in range(no_arguments):
        raw_value = data[slot * 64 : (slot + 1) * 64]

        if arguments_abi:
            if "name" in arguments_abi[i]:
                argument_name = arguments_abi[i]['name']
            else:
                argument_name = ''
            argument_type = arguments_abi[i]['type']
            if argument_type[:5] == "tuple":
                do_offset = arguments_abi[i]["dynamic"] or any(
                    a["dynamic"] for a in arguments_abi[i]["components"]
                )
                if do_offset:
                    offset = int(raw_value, 16) * 2
                    sub_arguments = data[offset:]
                else:
                    sub_arguments = data[i * 64 :]

                argument_value, slots = _decode_tuple(
                    sub_arguments, arguments_abi[i]['components'], argument_type[5:] == "[]"
                )

                if do_offset:
                    slot += 1
                else:
                    slot += slots

            elif argument_type == "bstring":
                argument_value = _decode_static_argument(raw_value, argument_type)
                slot += 1

            elif argument_type in ("bytes", "string"):
                offset = int(raw_value, 16) * 2
                argument_value = _decode_dynamic_argument(data[offset:], argument_type)
                slot += 1

            elif argument_type[-1:] == ']':
                array_type = argument_type[:-1].split("[")[0]

                if argument_type[-2:] == '[]':
                    offset = int(raw_value, 16) * 2
                    argument_value = _decode_dynamic_array(data[offset:], array_type)
                    slot += 1
                else:
                    array_size = int(argument_type[:-1].split("[")[1])
                    argument_value = []
                    for _ in range(array_size):
                        argument_value.append(_decode_static_argument(raw_value, array_type))
                        slot += 1
                        raw_value = data[slot * 64 : (slot + 1) * 64]

            else:
                argument_value = _decode_static_argument(raw_value, argument_type)
                slot += 1
        else:
            argument_name = "arg_%d" % (i + 1)
            argument_type = "unknown"
            argument_value = "0x" + raw_value

        if argument_type != "unknown" or argument_value:
            arguments_list.append(dict(name=argument_name, type=argument_type, value=argument_value))

    return arguments_list, slot


def decode_call(call_input, call_output, abi):
    function_name = ''
    function_arguments = []
    function_outputs = []

    function_signature = call_input[:10]
    if function_signature in abi:

        function_abi = abi[function_signature]
        function_name = function_abi['name']

        if call_input and call_input[10:]:
            function_arguments, _ = _decode_struct(call_input[10:], function_abi.get("inputs", None))

        if call_output and call_output[2:]:
            function_outputs, _ = _decode_struct(call_output[2:], function_abi.get("outputs", None))

    return function_name, function_arguments, function_outputs
