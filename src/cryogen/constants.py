from enum import Enum

PARTITION_SIZES = [1_000_000, 100_000, 10_000]
FAR_AWAY_BLOCK = 1_000_000_000
DEFAULT_RANGE = range(0, FAR_AWAY_BLOCK)
BLOCK_CHUNK = 1000
BATCH_SIZE = 32 * 2**20  # 32 mb uncompressed chunks


class Dataset(Enum):
    blocks = "blocks"
    transactions = "transactions"
    logs = "logs"
    events = "events"
    contracts = "contracts"
    traces = "traces"
    state_diffs = "state_diffs"
    balance_diffs = "balance_diffs"
    code_diffs = "code_diffs"
    nonce_diffs = "nonce_diffs"
    storage_diffs = "storage_diffs"
    vm_traces = "vm_traces"
    address_appearances = "address_appearances"
    balance_reads = "balance_reads"
    balances = "balances"
    code_reads = "code_reads"
    codes = "codes"
    erc20_balances = "erc20_balances"
    erc20_metadata = "erc20_metadata"
    erc20_supplies = "erc20_supplies"
    erc20_transfers = "erc20_transfers"
    eth_calls = "eth_calls"
    four_byte_counts = "four_byte_counts"
    geth_calls = "geth_calls"
    geth_code_diffs = "geth_code_diffs"
    geth_balance_diffs = "geth_balance_diffs"
    geth_storage_diffs = "geth_storage_diffs"
    geth_nonce_diffs = "geth_nonce_diffs"
    geth_opcodes = "geth_opcodes"
    javascript_traces = "javascript_traces"
    native_transfers = "native_transfers"
    nonce_reads = "nonce_reads"
    nonces = "nonces"
    slots = "slots"
    storages = "storages"
    trace_calls = "trace_calls"
    txs = "txs"
