from cryogen import consolidate
from cryogen.constants import FAR_AWAY_BLOCK


def test_consolidate():
    ranges = [range(start, start + 1000) for start in range(0, 2_222_000, 1000)]
    combined = consolidate.combine_ranges(ranges)
    assert list(combined) == [
        range(0, 1_000_000),
        range(1_000_000, 2_000_000),
        range(2_000_000, 2_100_000),
        range(2_100_000, 2_200_000),
        range(2_200_000, 2_210_000),
        range(2_210_000, 2_220_000),
        range(2_220_000, 2_221_000),
        range(2_221_000, 2_222_000),
    ]


def test_find_gaps():
    ranges = [range(1_000_000, 2_000_000), range(17_100_000, 17_200_000)]
    gaps = consolidate.find_gaps(ranges)
    assert gaps == [
        range(0, 1_000_000),
        range(2_000_000, 17_100_000),
        range(17_200_000, FAR_AWAY_BLOCK),
    ]
