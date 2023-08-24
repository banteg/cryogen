from pathlib import Path
from random import randrange
from tempfile import TemporaryDirectory

import pyarrow as pa
from cryogen import parquet
from pyarrow.parquet import write_table


def generate_table():
    return pa.table(
        [pa.array([randrange(256) for row in range(5)], type=pa.uint8()) for col in range(3)],
        names=["a", "b", "c"],
    )


def test_parquet_info():
    table = generate_table()

    with TemporaryDirectory() as dir:
        path = Path(dir) / "test.parquet"
        write_table(table, path)
        info = parquet.parquet_info(path)
        assert info["num_rows"] == 5


def test_parquet_merge():
    with TemporaryDirectory() as dir:
        tables = [generate_table() for table in range(5)]
        names = [Path(dir) / f"{i}.parquet" for i in range(5)]

        for table, name in zip(tables, names):
            write_table(table, name)

        out_name = Path(dir) / "out.parquet"
        parquet.merge_parquets(names, out_name)

        info_split = parquet.parquet_info(names)
        info_merged = parquet.parquet_info(out_name)

    assert info_split["num_rows"] == info_merged["num_rows"]
