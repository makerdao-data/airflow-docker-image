def decode_Approve(topic1, topic2, topic3, log_data):

    return {
        'token': str('0x' + topic1[26:66]),
        'spender': str('0x' + topic2[26:66]),
        'value': int(log_data, 16)
    }

def decode_Rely(topic1, topic2, topic3, log_data):

    return {'usr':  str('0x' + topic1[26:66])}

def decode_Deny(topic1, topic2, topic3, log_data):

    return {'usr':  str('0x' + topic1[26:66])}


def decode_event(topic0, topic1, topic2, topic3, log_data):

    # escrow
    if topic0 == '0x6e11fb1b7f119e3f2fa29896ef5fdf8b8a2d0d4df6fe90ba8668e7d8b2ffa25e':
        return 'Approve', decode_Approve(topic1, topic2, topic3, log_data)
    elif topic0 == '0xdd0e34038ac38b2a1ce960229778ac48a8719bc900b6c4f8d0475c6e8b385a60':
        return 'Rely', decode_Rely(topic1, topic2, topic3, log_data)
    elif topic0 == '0x184450df2e323acec0ed3b5c7531b81f9b4cdef7914dfd4c0a4317416bb5251b':
        return 'Deny', decode_Deny(topic1, topic2, topic3, log_data)
    else:
        return 'Unknown event', 'Unknown topic0'