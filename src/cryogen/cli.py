from typing import Annotated, Optional
from typer import Typer, Argument, Option, prompt
from pathlib import Path
from click import Choice
from enum import Enum
from cryogen.consolidate import find_gaps
from cryogen.utils import extract_range, parse_blocks
from cryogen.constants import FAR_AWAY_BLOCK

app = Typer()


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


@app.command()
def collect(
    dataset: Dataset,
    data_dir: Annotated[Path, Option(envvar="CRYO_DATA_DIR")],
    rpc_url: Annotated[str, Option(envvar="ETH_RPC_URL")],
    blocks: Annotated[Optional[range], Option(parser=parse_blocks)] = None,
):
    print(dataset.value, data_dir, rpc_url)
    dataset_dir = data_dir / dataset.value

    # blocks = parse_blocks(blocks)
    print(blocks)

    # split works into gaps
    ranges = [extract_range(file) for file in dataset_dir.glob("*.parquet")]
    gaps = find_gaps(ranges)
    print(gaps)


if __name__ == "__main__":
    app()
