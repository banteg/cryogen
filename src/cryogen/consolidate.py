from cryogen.constants import PARTITION_SIZES


def combine_ranges(
    ranges: list[range],
) -> dict[range, list[range]]:
    """
    combine ranges into bigger ranges. only merge

    returns {big_range: [ranges, to, be, merged]}
    """
    ranges = sorted(ranges, key=lambda r: r.start)
    consumed_ranges = set()
    combined_ranges = {}

    # try parition sizes from large to small
    for partition_size in sorted(PARTITION_SIZES, reverse=True):
        # find starting points that align with a partition size
        possible_ranges = [
            range(r.start, r.start + partition_size)
            for r in ranges
            if r.start % partition_size == 0 and r not in consumed_ranges
        ]
        for possible_range in possible_ranges:
            chunks = []
            for r in ranges:
                if r in consumed_ranges:
                    # chunk was already merged into a bigger partition
                    continue
                if r.start in possible_range and (r.stop - 1) in possible_range:
                    chunks.append(r)
            # if there is no gaps, we can merge these chunks
            # this also makes sure we don't merge before a bigger chunk is ready
            if sum(len(c) for c in chunks) == len(possible_range):
                combined_ranges[possible_range] = chunks
                consumed_ranges.update(chunks)

    # add leftover ranges
    for r in ranges:
        if r not in consumed_ranges:
            combined_ranges[r] = [r]

    return combined_ranges
