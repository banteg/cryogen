from enum import Enum
from pathlib import Path
from time import time
from typing import Annotated, Optional

import cryo
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.theme import Theme
from tqdm import tqdm
from typer import Argument, Option, Typer, prompt

from cryogen.consolidate import combine_ranges, find_gaps
from cryogen.constants import DEFAULT_RANGE, FAR_AWAY_BLOCK
from cryogen.parquet import merge_parquets, parquet_info
from cryogen.utils import extract_range, parse_blocks, replace_range

app = Typer()
console = Console(theme=Theme({"repr.number": "bold green"}))


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
    blocks: Annotated[range, Option(parser=parse_blocks)] = DEFAULT_RANGE,
):
    print(dataset.value, data_dir, rpc_url)
    dataset_dir = data_dir / dataset.value

    console.log(blocks)

    # split works into gaps
    ranges = [extract_range(file) for file in dataset_dir.glob("*.parquet")]
    gaps = find_gaps(ranges)
    console.log("gaps", gaps)

    for gap in gaps:
        adjusted = range(
            max(gap.start, blocks.start),
            min(gap.stop, blocks.stop),
        )
        block_range = f"{adjusted.start}:"
        if adjusted.stop != FAR_AWAY_BLOCK:
            block_range += f"{adjusted.stop}"

        cryo.freeze(
            dataset.value,
            blocks=[block_range],
            align=True,
            output_dir=str(dataset_dir),
            compression=["zstd", "3"],
        )


@app.command()
def consolidate(
    dataset: Dataset,
    data_dir: Annotated[Path, Option(envvar="CRYO_DATA_DIR")],
    inplace: bool = True,
):
    dataset_dir = data_dir / dataset.value
    suffix = "" if inplace else "_out"
    output_dir = data_dir / (dataset.value + suffix)
    output_dir.mkdir(parents=True, exist_ok=True)

    sample_name = next(dataset_dir.glob("*.parquet"))
    ranges = [extract_range(file) for file in dataset_dir.glob("*.parquet")]
    combined = combine_ranges(ranges, leftover=False)
    progress = Progress(console=console)
    overall_task = progress.add_task("consolidate", total=len(ranges))

    with progress:
        for r in combined:
            input_files = [replace_range(sample_name, sub) for sub in combined[r]]
            output_file = output_dir / replace_range(sample_name, r).name

            merge_parquets(input_files, output_file)

            # delete input files
            if inplace:
                for f in input_files:
                    f.unlink()

            progress.update(overall_task, advance=len(combined[r]))

    info_out = parquet_info(output_dir)
    console.log(info_out)


if __name__ == "__main__":
    app()
