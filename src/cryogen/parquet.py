from collections import Counter
from time import time

from pyarrow.dataset import dataset as arrow_dataset
from pyarrow.parquet import FileMetaData, ParquetDataset, ParquetFile, ParquetWriter


def parquet_info(files: str | list[str]) -> dict:
    start = time()
    dataset: ParquetDataset = arrow_dataset(files, format="parquet")
    info = Counter()
    info["num_rows"] = dataset.count_rows()
    for file in dataset.files:
        meta: FileMetaData = ParquetFile(file).metadata
        info["files"] += 1
        info["row_groups"] += meta.num_row_groups
        for key in ["total_compressed_size", "total_uncompressed_size"]:
            info[key] += sum(
                getattr(meta.row_group(row).column(col), key)
                for row in range(meta.num_row_groups)
                for col in range(meta.num_columns)
            )

    info["elapsed"] = round(time() - start, 3)
    return dict(info)


def merge_parquets(files: list[str], output: str):
    assert len(files) > 1, "trying to merge a single file"

    dataset: ParquetDataset = arrow_dataset(files, format="parquet")

    with ParquetWriter(output, dataset.schema, compression="zstd", compression_level=3) as writer:
        # there can be more batches when merging files containing over 2**20 rows
        for batch in dataset.to_batches():
            writer.write_batch(batch)
