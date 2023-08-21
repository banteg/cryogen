from cryogen import consolidate


def test_consolidate():
    ranges = [range(start, start + 1000) for start in range(0, 2_222_001, 1000)]
    combined = consolidate.combine_ranges(ranges)
    assert combined == [
        range(0, 1_000_000),
        range(1_000_000, 2_000_000),
        range(2_000_000, 2_100_000),
        range(2_100_000, 2_200_000),
        range(2_200_000, 2_210_000),
        range(2_210_000, 2_220_000),
        range(2_220_000, 2_210_000),
        range(2_221_000, 2_220_000),
    ]
