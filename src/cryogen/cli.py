from time import time
from typing import Annotated, Optional
from typer import Typer, Argument, Option, prompt
from pathlib import Path
from click import Choice
from enum import Enum
from cryogen.consolidate import find_gaps, combine_ranges
from cryogen.utils import extract_range, parse_blocks, replace_range
from cryogen.parquet import merge_parquets, parquet_info, MergeMethod
from cryogen.constants import FAR_AWAY_BLOCK
from rich import print

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


@app.command()
def consolidate(
    dataset: Dataset,
    data_dir: Annotated[Path, Option(envvar="CRYO_DATA_DIR")],
    suffix: str = "out",
    method: MergeMethod = MergeMethod.batches,
):
    print(dataset.value, data_dir)
    dataset_dir = data_dir / dataset.value
    output_dir = data_dir / (dataset.value + f"_{suffix}")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(dataset_dir)
    info = parquet_info(dataset_dir)
    print(info)

    sample_name = next(dataset_dir.glob("*.parquet"))
    ranges = [extract_range(file) for file in dataset_dir.glob("*.parquet")]
    combined = combine_ranges(ranges)
    start = time()

    for r in combined:
        # print(f"consolidating {r} from {len(combined[r])} files")
        output_file = output_dir / replace_range(sample_name, r).name
        input_files = [replace_range(sample_name, sub) for sub in combined[r]]
        # print(output_file)
        merge_parquets(input_files, output_file, method)

    info_out = parquet_info(output_dir)
    print(info_out)
    print(f"elapsed {time() - start:.3f}")


if __name__ == "__main__":
    app()
