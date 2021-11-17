from web3 import Web3


def _get_chief(chain, vote_delegate):
    ABI = [{"inputs":[],"name":"chief","outputs":[{"internalType":"contract ChiefLike","name":"","type":"address"}],"stateMutability":"view","type":"function"}]

    vote_delegate_contract = chain.eth.contract(
        address=Web3.toChecksumAddress(vote_delegate), abi=ABI
    )

    chief_address = vote_delegate_contract.functions.chief().call()

    return chief_address


def _get_deposits(chain, chief, address, block):
    ABI = '[{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"deposits","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'

    chief_contract = chain.eth.contract(
        address=Web3.toChecksumAddress(chief), abi=ABI
    )

    balance = chief_contract.functions.deposits(
        Web3.toChecksumAddress(address)
    ).call(block_identifier=block)

    return balance / 10 ** 18