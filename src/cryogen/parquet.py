import shutil
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
    if len(files) == 1 and str(files[0]) != str(output):
        shutil.copyfile(files[0], output)
        return

    dataset: ParquetDataset = arrow_dataset(files, format="parquet")
    size = parquet_info(files)["total_compressed_size"]

    with ParquetWriter(output, dataset.schema, compression="zstd", compression_level=3) as writer:
        if size > 2**28:  # 256mb
            # there can be more batches when merging files containing over 2**20 rows
            n = 0
            for batch in dataset.to_batches():
                writer.write_batch(batch)
                n += 1
            print(f"written as {n} row groups")
        else:
            # write as a singe row group
            writer.write_table(dataset.to_table())
            print("written as a single row group")
