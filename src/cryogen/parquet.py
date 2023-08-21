from pyarrow.dataset import dataset as arrow_dataset
from pyarrow.parquet import ParquetDataset, ParquetFile, FileMetaData
from collections import Counter


def parquet_info(files: str | list[str]) -> dict:
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

    return info
