def decode_Rely(topic1, topic2, topic3, log_data):

    return {'usr':  str('0x' + topic1[26:66])}

def decode_Deny(topic1, topic2, topic3, log_data):

    return {'usr':  str('0x' + topic1[26:66])}

def decode_LogCeiling(topic1, topic2, topic3, log_data):

    return {'ceiling': int(log_data, 16)}

def decode_LogMaxDeposit(topic1, topic2, topic3, log_data):

    return {'maxDeposit': int(log_data, 16)}

def decode_LogDeposit(topic1, topic2, topic3, log_data):

    return {'l1Sender': str('0x' + topic1[26:66]), 'amount': int(str('0x' + log_data[2:66]), 16), 'l2Recipient': str('0x' + log_data[66:130])}

def decode_LogWithdrawal(topic1, topic2, topic3, log_data):

    return {'l1Recipient': str('0x' + topic1[26:66]), 'amount': int(log_data, 16)}

def decode_LogForceWithdrawal(topic1, topic2, topic3, log_data):

    return {'l1Recipient': str('0x' + topic1[26:66]), 'amount': int(log_data, 16), 'l2Sender': topic2}

def decode_LogStartDepositCancellation(topic1, topic2, topic3, log_data):

    return {'l2Recipient': topic1, 'amount': int(str('0x' + log_data[2:66]), 16), 'nonce': int(str('0x' + log_data[66:130]), 16)}

def decode_LogCancelDeposit(topic1, topic2, topic3, log_data):

    return {
        'l2Recipient': topic1,
        'l1Recipient': str('0x' + log_data[26:66]),
        'amount': int(str('0x' + log_data[66:130]), 16),
        'nonce': int(str('0x' + log_data[130:196]), 16)
    }


def decode_event(topic0, topic1, topic2, topic3, log_data):

    # bridge
    if topic0 == '0xdd0e34038ac38b2a1ce960229778ac48a8719bc900b6c4f8d0475c6e8b385a60':
        return 'Rely', decode_Rely(topic1, topic2, topic3, log_data)
    elif topic0 == '0x184450df2e323acec0ed3b5c7531b81f9b4cdef7914dfd4c0a4317416bb5251b':
        return 'Deny', decode_Deny(topic1, topic2, topic3, log_data)
    elif topic0 == '0x1cdde67b72a90f19919ac732a437ac2f7a10fc128d28c2a6e525d89ce5cd9d3a':
        return 'Close', {'isOpen': False}
    elif topic0 == '0x6defc6f2eb7fe7d2a05d39d89d53405300c4dafb0e9cd1d6affeb7c02a9c3e54':
        return 'LogCeiling', decode_LogCeiling(topic1, topic2, topic3, log_data)
    elif topic0 == '0x0abf56f125eb3b9ec6b166b22f262406810c29da2da4c902a6ee31694ae11a39':
        return 'LogMaxDeposit', decode_LogMaxDeposit(topic1, topic2, topic3, log_data)
    elif topic0 == '0x9dbb0e7dda3e09710ce75b801addc87cf9d9c6c581641b3275fca409ad086c62':
        return 'LogDeposit', decode_LogDeposit(topic1, topic2, topic3, log_data)
    elif topic0 == '0xb4214c8c54fc7442f36d3682f59aebaf09358a4431835b30efb29d52cf9e1e91':
        return 'LogWithdrawal', decode_LogWithdrawal(topic1, topic2, topic3, log_data)
    elif topic0 == '0xdee288762e02cf1a2e99896626b9675625e9fa32cce23d9ee7d490763436eaa3':
        return 'LogForceWithdrawal', decode_LogForceWithdrawal(topic1, topic2, topic3, log_data)
    elif topic0 == '0xb8b6bc18e48f410a36e8867df19f26eb867bad25616833b0ed9141f6d8933929':
        return 'LogStartDepositCancellation', decode_LogStartDepositCancellation(topic1, topic2, topic3, log_data)
    elif topic0 == '0x27342a36c014a937136f67690b80039f954cc7acd1d6a2f5bca3f3d3e7b94837':
        return 'LogCancelDeposit', decode_LogCancelDeposit(topic1, topic2, topic3, log_data)
    else:
        return 'Unknown event', 'Unknown topic0'