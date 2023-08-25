# cryogen

helps you preserve the ethereum dataset fresh, fast and small

## install

```shell
pip install cryogen
```

## features

intelligently consolidates cryo-extracted datasets, cutting the number of chunks by 400-1000x.

offers fast in-place conversion that reduces disk footprint by 2x and increases query performance.

keeps the dataset fresh so you can always come back to up-to-date data.

## usage

### `cryogen collect <dataset>`

collect or update a cryo dataset.

cryogen uses 1000 block batches with zstd -3 compression. the gaps are filled automatically. the dataset can be up to 1000 blocks behind head beacuse the align option is used.

```shell
# you can also specify data_dir using CRYO_DATA_DIR env var
cryogen collect contracts --data-dir ~/cryo_data

# collect a block range, same format as cryo
cryogen collect traces --blocks 17000000:
```

### `cryogen consolidate <dataset>`

consolidate a dataset in-place.

this command will merge parquets into larger files covering 1e4, 1e5, 1e6 blocks. smaller files are not touched until a larger contiguous block can be formed. the worst case for this algorithm is 17 + 9 + 9 + 9 = 44 files at block 17,999,000.

starting from v0.2.0 cryogen also reduces the number of row groups. it creates a row group when an uncompressed size of a batch reaches 32 mb. this row group size provides optimal performance without imposing higher memory requirements that could lead to heavy swapping.

```shell
cryogen consolidate contracts

# test the feature without overwriting the dataset
cryogen consolidate contraces --no-inplace
```

note that after consolidating with cryogen, you should use cryogen to update the dataset. cryo won't recognize larger chunks and would attempt to collect the already merged and deleted smaller chunks, outputting duplicate data in the dataset.

### `cryogen watch <dataset>`

combines `collect` and `consolidate` command.

keep it running and it will update the dataset periodically.

```shell
# refresh every 4 hours
cryogen watch contracts --interval 14400
```

### `cryogen info <dataset>`

collects info about a parquet dataset.

```shell
cryogen info contracts
# {'num_rows': 62466632, 'files': 38, 'row_groups': 17984, 'total_compressed_size': 7850356027, 'total_uncompressed_size': 29236070746, 'elapsed': 0.747}
```
