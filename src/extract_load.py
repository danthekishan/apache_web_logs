from itertools import islice
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

    args:
        paths - paths to read (iter object)

    return:
        seq of files
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

    args:
        files -  files to read (iter object)

    return:
        seq of content
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

    args:
        dict_seq - seq object
        name - column name
        func - func to be applied
    """
    for _dict in dict_seq:
        _dict[name] = func(_dict[name])
        yield _dict


def chunk_gen(log_gen, chunk_size):
    """
    Chunk iterable dataset

    args:
        log_gen - log sequence
        chunk_size - size of the chunk

    return:
        sequence of pyarrow record batch
    """
    log_gen = iter(log_gen)
    for first in log_gen:  # stops when iterator is depleted

        def chunk():  # construct generator for next chunk
            yield first  # yield element from for loop
            for more in islice(log_gen, chunk_size - 1):
                yield more  # yield more elements from the iterator

        arr = pa.array(chunk())
        logging.info(f"chunk of data - {len(arr)}")
        yield pa.RecordBatch.from_struct_array(arr)


def write_to_paruqet_from_batch_gen(log_gen, filename, schema, size=100_000):
    """
    Writing to a parquet file from log sequnce

    args:
        log_gen - sequence of logs
        filename - parquet filename
        schema - pyarrow schema
        size - chunk size
    """
    with pq.ParquetWriter(filename, schema=schema) as writer:
        for batch in chunk_gen(log_gen, size):
            writer.write_batch(batch)
            ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
            logging.info(
                "Allocated memory by pyarrow {} after chunk is written to parquet".format(
                    ram
                )
            )


def get_dataset(dir_name):
    return ds.dataset(dir_name, format="parquet")


def get_dataset_with_partition(dir_name, partition_fields):
    return ds.dataset(dir_name, partitioning=partition_fields)


def create_arrow_table_from_log_seq(log_seq, schema):
    """
    Create a new table that consist of record batch (size of 100_000)

    args:
        log_seq - log sequence
        schema - pyarrow schema

    return:
        pyarrow table
    """
    output = []
    data = []
    for idx, line in enumerate(log_seq, start=1):
        if idx % 100_000 == 0:
            output.append(pa.RecordBatch.from_struct_array(pa.array(data)))
            data = []
        data.append(line)

    # creating record batch from rest of the data
    output.append(pa.RecordBatch.from_struct_array(pa.array(data)))
    arr_table = pa.Table.from_batches(
        output,
        schema=schema,
    )
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.info(
        "Allocated memory by pyarrow {} after creating arrow table from apache logs".format(
            ram
        )
    )

    return arr_table


def write_log_seq_to_parquet_dataset(apache_log_seq, dir, schema, partition_field):
    """
    Write back to the pyarrow table to parquet dataset with partitioning

    args:
        apache_log_seq - apache log sequence
        dir - directory to be written
        schema - pyarrow schema
        partition_field - partitioning field name
    """
    table = create_arrow_table_from_log_seq(apache_log_seq, schema)
    pq.write_to_dataset(
        table,
        root_path=dir,
        partition_cols=partition_field,
    )
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.info(
        "Allocated memory by pyarrow {} after writing into parquet dataset".format(ram)
    )


def write_log_chunks_to_parquet_dataset(apache_log_seq, ds_root_dir, schema):
    """
    creating parquet dataset from log sequences

    args:
        apache_log_seq - log sequence
        ds_root_dir - root directory for dataset
        schema - pyarrow schema

    return:
        dataset obj
    """
    DATA = []
    batch = 0
    # starting from 1 to avoid 0 % n = 0
    for idx, line in enumerate(apache_log_seq, start=1):
        try:
            if idx % 100_000 == 0:
                pq.write_to_dataset(
                    pa.Table.from_struct_array(pa.array(DATA)),
                    root_path=f"{ds_root_dir}/log{batch}.parquet",
                    schema=schema,
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
        schema=schema,
    )

    return ds.dataset(ds_root_dir)


def repartitioned_parquet_dataset(dataset, output_dataset_folder, partition_field):
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
    # dataset = create_parquet_dataset_log(gen, output_dataset_folder, schema)
    ds.write_dataset(
        dataset,
        output_dataset_folder,
        partitioning=ds.partitioning(
            pa.schema([dataset.schema.field(partition_field)])
        ),
    )
