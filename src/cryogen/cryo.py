import re
from pathlib import Path


def extract_range(file: str) -> range:
    start_block, stop_block = re.findall(r"\d{7,}", Path(file).stem)
    return range(int(start_block), int(stop_block) + 1)


def replace_range(file: str, r: range):
    name = re.sub(r"\d+_to_\d+", f"{r.start:08d}_to_{r.stop:08d}", Path(file))
    return Path(file).with_stem(name)
