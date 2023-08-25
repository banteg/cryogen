import shutil
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from time import time

from pyarrow import RecordBatch, Table
from pyarrow.dataset import dataset as arrow_dataset
from pyarrow.parquet import FileMetaData, ParquetDataset, ParquetFile, ParquetWriter
from rich import print

from cryogen.constants import BATCH_SIZE


@dataclass
class PendingBatch:
    batches: list[RecordBatch] = field(default_factory=list)
    size: int = 0
    rows: int = 0

    def add(self, batch: RecordBatch):
        self.batches.append(batch)
        self.size += batch.get_total_buffer_size()
        self.rows += batch.num_rows

    def clear(self):
        del self.batches[:]
        self.size = 0
        self.rows = 0

    def to_table(self) -> Table:
        return Table.from_batches(self.batches)

    def __str__(self):
        return f"<PendingBatch chunks={len(self.batches)} size={self.size / 2**20:,.2f}mb rows={self.rows:,d}>"  # noqa: E501


def parquet_info(files: Path | list[Path]) -> dict:
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

    info["elapsed"] = time() - start
    return dict(info)


def merge_parquets(files: list[Path], output: Path):
    inplace = files[0].parent == output.parent
    # copy a single chunk if we are not combining in-place
    if len(files) == 1:
        if inplace:
            print("  skip file with a single chunk")
        else:
            print("  copying file")
            shutil.copyfile(files[0], output)

        return

    dataset: ParquetDataset = arrow_dataset(files, format="parquet")
    temp_output = output.with_suffix(".tmp")

    with ParquetWriter(
        temp_output, dataset.schema, compression="zstd", compression_level=3
    ) as writer:
        pending_batch = PendingBatch()
        row_groups = 0
        # files containing over 2**20 rows can produce multiple batches
        for batch in dataset.to_batches():  # type: ignore
            pending_batch.add(batch)
            # write row group
            if pending_batch.size > BATCH_SIZE:
                print(f"  writing {pending_batch}")
                writer.write_table(pending_batch.to_table())
                pending_batch.clear()
                row_groups += 1

        if pending_batch.batches:
            print(f"  writing {pending_batch}")
            writer.write_table(pending_batch.to_table())
            row_groups += 1

        temp_output.rename(output)
        print(f"[green]written as {row_groups} row groups")

        if inplace:
            print(f"[red]deleting {len(files)} files")
            for file in files:
                file.unlink()
