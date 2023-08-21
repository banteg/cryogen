import re
from pathlib import Path

from cryogen.constants import FAR_AWAY_BLOCK


def extract_range(file: str) -> range:
    start_block, stop_block = re.findall(r"\d+", Path(file).stem)
    return range(int(start_block), int(stop_block) + 1)


def replace_range(file: str, r: range) -> str:
    name = re.sub(r"\d+_to_\d+", f"{r.start:08d}_to_{r.stop:08d}", Path(file).stem)
    return str(Path(file).with_stem(name))


def parse_blocks(blocks):
    if blocks in [None, ":"]:
        return range(0, FAR_AWAY_BLOCK)

    blocks_pattern = r"^(\d+)?:(\d+)?$"
    match = re.search(blocks_pattern, blocks)
    if not match:
        raise ValueError("blocks must be either specified as start_block:stop_block")

    start_block = int(match.group(1) or 0)
    stop_block = int(match.group(2) or FAR_AWAY_BLOCK)

    return range(start_block, stop_block)
