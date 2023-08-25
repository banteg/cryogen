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
    contracts = "contracts"
    traces = "traces"
    state_diffs = "state_diffs"
    balance_diffs = "balance_diffs"
    code_diffs = "code_diffs"
    nonce_diffs = "nonce_diffs"
    storage_diffs = "storage_diffs"
    vm_traces = "vm_traces"
