import time
from enum import Enum
from pathlib import Path
from typing import Annotated

import cryo
from typer import Option, Typer

from cryogen.consolidate import combine_ranges, find_gaps
from cryogen.constants import DEFAULT_RANGE, FAR_AWAY_BLOCK
from cryogen.parquet import merge_parquets, parquet_info
from cryogen.utils import extract_range, parse_blocks, replace_range

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
    blocks: Annotated[range, Option(parser=parse_blocks)] = DEFAULT_RANGE,
):
    dataset_dir = data_dir / dataset.value

    # split works into gaps
    ranges = [extract_range(file) for file in dataset_dir.glob("*.parquet")]
    gaps = find_gaps(ranges)

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

    for r in combined:
        if len(combined[r]) == 1:
            continue

        input_files = [replace_range(sample_name, sub) for sub in combined[r]]
        output_file = output_dir / replace_range(sample_name, r).name

        print(f"combining {output_file.name} from {len(input_files)} files")
        merge_parquets(input_files, output_file)

        print("cleaning up")
        if inplace:
            for f in input_files:
                f.unlink()

        print("done")


@app.command()
def watch(
    dataset: Dataset,
    data_dir: Annotated[Path, Option(envvar="CRYO_DATA_DIR")],
    blocks: Annotated[range, Option(parser=parse_blocks)] = DEFAULT_RANGE,
    interval: Annotated[int, Option(min=60, max=86400)] = 3600,
):
    while True:
        collect(dataset, data_dir, blocks)
        consolidate(dataset, data_dir, inplace=True)
        time.sleep(interval)


if __name__ == "__main__":
    app()
