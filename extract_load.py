import logging
import gzip
from pathlib import Path
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds


def open_files(paths):
    """
    Open matched paths by its file type
    class and yield back sequence of objs
    """
    for path in paths:
        if path.suffix == ".gz":
            yield gzip.open(path, "rt")
        else:
            yield open(path, "rt")


def read_content(files):
    """
    yield content of file from
    sequence of open file objs
    """
    for file in files:
        yield from file


def read_files(directory, file_pattern):
    """
    Accept multiple files (using pattern to identify)
    or single file

    args:
        directory - root folder
        file_pattern - pattern of files to be read

    return:
        seq of lines(read)
    """
    paths = Path(directory).rglob(file_pattern)
    files = open_files(paths)
    lines = read_content(files)
    return lines


def map_function_to_element(dict_seq, name, func):
    """
    mapping a function to an element
    """
    for _dict in dict_seq:
        _dict[name] = func(_dict[name])
        yield _dict


def create_arrow_table(apache_log_seq):
    """
    Create a new table that consist of
    record batch that size of 1_000_000
    """
    output = []
    data = []
    for idx, line in enumerate(apache_log_seq, start=1):
        if idx % 1_000_000 == 0:
            output.append(pa.RecordBatch.from_struct_array(pa.array(data)))
            data = []
        data.append(line)
    arr_table = pa.Table.from_batches(output)
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.INFO(
        "Allocated memory by pyarrow {} after creating arrow table from apache logs".format(
            ram
        )
    )

    return arr_table


def write_arrow_table_to_partitioned_parquet(arr_table, dir, partition_field):
    """
    Write back to the pyarrow parquet dataset
    """
    pq.write_to_dataset(arr_table, root_path=dir, partition_cols=[partition_field])
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.INFO(
        "Allocated memory by pyarrow {} after writing into parquet dataset".format(ram)
    )


def create_parquet_dataset_log(apache_log_seq, ds_root_dir):
    """
    creating parquet dataset from log sequences

    args:
        apache_log_seq - log sequence
        ds_root_dir - root directory for dataset

    return:
        dataset obj
    """
    DATA = []
    batch = 0
    # starting from 1 to avoid 0 % n = 0
    for idx, line in enumerate(apache_log_seq, start=1):
        if idx % 1000000 == 0:
            pq.write_to_dataset(
                pa.Table.from_pandas(pd.DataFrame(DATA)),
                root_path=f"{ds_root_dir}/log{batch}.parquet",
            )
            DATA = []
            batch += 1
        DATA.append(line)

    # final dataset that dont fall under 1_000_000
    pq.write_to_dataset(
        pa.Table.from_pandas(pd.DataFrame(DATA)),
        root_path=f"{ds_root_dir}/log{batch}.parquet",
    )

    return ds.dataset(ds_root_dir)


def re_parition_dataset(dataset, output_dataset_folder, partition_field):
    """
    Partition the dataset again accoring to custom
    partition field

    args:
        dataset - current dataset
        output_dataset_folder - new dataset folder
        partition_field - field from existing dataset

    return:
        dataset obj
    """
    ds.write_dataset(
        dataset,
        output_dataset_folder,
        partitioning=ds.partitioning(
            pa.schema([dataset.schema.field(partition_field)])
        ),
    )
    return ds.dataset(output_dataset_folder)


def get_dataset(dir_name):
    return ds.dataset(dir_name)
