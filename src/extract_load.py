import gzip
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


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
        if idx % 100_000 == 0:
            output.append(pa.RecordBatch.from_struct_array(pa.array(data)))
            data = []
        data.append(line)

    # creating record batch from rest of the data
    # output.append(pa.RecordBatch.from_struct_array(pa.array(data)))
    arr_table = pa.Table.from_batches(
        output,
        schema=pa.schema(
            [
                ("bytes", pa.int64()),
                ("datetime", pa.timestamp("us")),
                ("host", pa.string()),
                ("http_referred", pa.string()),
                ("method", pa.string()),
                ("proto", pa.string()),
                ("referrer", pa.string()),
                ("request", pa.string()),
                ("status", pa.int64()),
                ("user", pa.string()),
                ("user_agent", pa.string()),
            ]
        ),
    )
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.info(
        "Allocated memory by pyarrow {} after creating arrow table from apache logs".format(
            ram
        )
    )

    return arr_table


def write_arrow_table_to_partitioned_parquet(apache_log_seq, dir, partition_field):
    """
    Write back to the pyarrow parquet dataset
    """
    table = create_arrow_table(apache_log_seq)
    pq.write_to_dataset(
        table,
        root_path=dir,
        partition_cols=partition_field,
    )
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.info(
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
        try:
            if idx % 1_000_000 == 0:
                pq.write_to_dataset(
                    pa.Table.from_struct_array(pa.array(DATA)),
                    root_path=f"{ds_root_dir}/log{batch}.parquet",
                    schema=pa.schema(
                        [
                            ("bytes", pa.int64()),
                            ("datetime", pa.timestamp("us")),
                            ("host", pa.string()),
                            ("http_referred", pa.string()),
                            ("method", pa.string()),
                            ("proto", pa.string()),
                            ("referrer", pa.string()),
                            ("request", pa.string()),
                            ("user", pa.string()),
                            ("user_agent", pa.string()),
                            ("status", pa.string()),
                        ]
                    ),
                )
                DATA = []
                batch += 1
            DATA.append(line)
        except Exception as e:
            logging.error(f"Error while looping - {e}")

    # final dataset that dont fall under 1_000_000
    pq.write_to_dataset(
        pa.Table.from_struct_array(pa.array(DATA)),
        root_path=f"{ds_root_dir}/log{batch}.parquet",
        schema=pa.schema(
            [
                ("bytes", pa.int64()),
                ("datetime", pa.timestamp("us")),
                ("host", pa.string()),
                ("http_referred", pa.string()),
                ("method", pa.string()),
                ("proto", pa.string()),
                ("referrer", pa.string()),
                ("request", pa.string()),
                ("user", pa.string()),
                ("user_agent", pa.string()),
                ("status", pa.string()),
            ]
        ),
    )

    return ds.dataset(ds_root_dir)


def partitioned_dataset_from_log(gen, output_dataset_folder, partition_field):
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
    dataset = create_parquet_dataset_log(gen, output_dataset_folder)
    ds.write_dataset(
        dataset,
        output_dataset_folder,
        partitioning=ds.partitioning(
            pa.schema([dataset.schema.field(partition_field)])
        ),
    )


def get_dataset(dir_name, partition_fields):
    return ds.dataset(dir_name, partitioning=partition_fields)
