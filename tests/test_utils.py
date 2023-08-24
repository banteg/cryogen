from pathlib import Path

import pytest
from cryogen import utils
from cryogen.constants import BLOCK_CHUNK, FAR_AWAY_BLOCK

name = Path("ethereum__contracts__12325000_to_12325999.parquet")


def test_extract_range():
    assert utils.extract_range(name) == range(12325000, 12326000)


def test_replace_range():
    new_name = Path("ethereum__contracts__00001000_to_00001999.parquet")
    assert utils.replace_range(name, range(1000, 2000)) == new_name


@pytest.mark.parametrize(
    ["blocks", "expected"],
    [
        [None, range(0, FAR_AWAY_BLOCK)],
        [":", range(0, FAR_AWAY_BLOCK)],
        [":1000", range(0, 1000)],
        ["2000:", range(2000, FAR_AWAY_BLOCK)],
        ["3000:4000", range(3000, 4000)],
    ],
)
def test_parse_blocks(blocks, expected):
    assert utils.parse_blocks(blocks) == expected


@pytest.mark.parametrize(
    "blocks",
    ["/", "1000:2000:3000", "-1000:", "4000:3000", "1234:4567"],
)
def test_parse_blocks_err(blocks):
    with pytest.raises(ValueError):
        utils.parse_blocks(blocks)
