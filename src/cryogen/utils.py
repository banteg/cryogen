import re
from pathlib import Path

from cryogen.constants import BLOCK_CHUNK, DEFAULT_RANGE, FAR_AWAY_BLOCK


def extract_range(file: Path) -> range:
    # cryo uses [start, stop], convert to python [start, stop)
    start_block, stop_block = re.findall(r"\d+", file.stem)[-2:]
    return range(int(start_block), int(stop_block) + 1)


def replace_range(file: Path, r: range) -> Path:
    # convert back from [start, stop) to [start, stop]
    name = re.sub(r"\d+_to_\d+", f"{r.start:08d}_to_{r.stop - 1:08d}", file.stem)
    return file.with_stem(name)


def parse_blocks(blocks) -> range:
    if blocks in [None, ":", DEFAULT_RANGE]:
        return DEFAULT_RANGE

    blocks_pattern = r"^(\d+)?:(\d+)?$"
    match = re.search(blocks_pattern, blocks)
    if not match:
        raise ValueError("blocks must be either specified as start:stop")

    start_block = int(match.group(1) or 0)
    stop_block = int(match.group(2) or FAR_AWAY_BLOCK)
    if start_block > stop_block:
        raise ValueError("stop block must be higher than start block")
    if stop_block % BLOCK_CHUNK != 0 or stop_block % BLOCK_CHUNK != 0:
        raise ValueError(f"blocks must be a multiple of {BLOCK_CHUNK}")

    return range(start_block, stop_block)
