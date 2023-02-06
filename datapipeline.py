from time import perf_counter
from datetime import datetime
import re
from typing import Generator
from extract_load import (
    read_logs,
    map_function_to_element,
    create_parquet_dataset_log,
    re_parition_dataset,
)
import duckdb
from transform_load import join_with_hostnames, load_to_duckdb


class DataPipeline:
    def __init__(self, type, database):
        self.type = type
        self.database = database

    def __enter__(self):
        self.con = duckdb.connect(database=self.database, read_only=True)
        if self.type == "apache_web_log":
            self.datapipeline = ApacheDataPipeline(self.con)
        else:
            raise ValueError("Incorrect datapipeline type")
        return self.datapipeline

    def __exit__(self, *args, **kwargs):
        self.con.close()


class ApacheDataPipeline:
    def __init__(self, database_connection):
        self.hostname_csv = None
        self.log_gen: Generator | None = None
        self.dataset = None
        self.final_table = None
        self.con = database_connection

    def extract_log(self, directory, file_pattern, hostname_csv):
        """
        read apache web server logs

        args:
            directory - file located directory
            file_pattern - patter for read files or file

        return:
            seq of logs
        """
        # setting hostname csv for later usage
        self.hostname_csv = hostname_csv

        # extract log process
        line_pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(\S+) (\S+) (\S+)" (\S+) (\S+) "(.*?)" "(.*?)"'
        log_line_pattern = re.compile(line_pattern)
        cols = (
            "host",
            "referrer",
            "user",
            "datetime",
            "method",
            "request",
            "proto",
            "status",
            "bytes",
            "http_referred",
            "user_agent",
        )

        lines = read_logs(directory, file_pattern)

        # spliting by regex
        groups = (log_line_pattern(line) for line in lines)  # type: ignore
        matched = (g.groups() for g in groups if g)

        # creating a key, value pair
        self.log_gen = (dict(zip(cols, m)) for m in matched)
        return self

    def clean_log_record(self):
        """
        clean log
        """
        # self.log_gen cannot be None
        if self.log_gen is None:
            raise ValueError("self.log_gen cannot be None, extract_log first")

        # cleaning data
        log_line = self.log_gen
        log_line = map_function_to_element(
            log_line, "bytes", lambda b: int(b) if b != "-" else 0
        )
        log_line = map_function_to_element(log_line, "status", int)
        log_line = map_function_to_element(
            log_line,
            "datetime",
            lambda d: datetime.strptime(d, "%d/%b/%Y:%H:%M:%S +%f")
            if d
            else datetime(1900, 1, 1),
        )
        self.log_gen = log_line
        return self

    def load_to_data_lake(
        self,
        dataset_folder="log_datalake",
        re_partition=False,
        new_dataset_folder=None,
        partition_field=None,
    ):
        """
        load data to data lake as parquet files
        """
        if re_partition and (new_dataset_folder is None or partition_field is None):
            raise ValueError(
                "new_dataset_folder and partition_field both should have values"
            )

        if self.log_gen is None:
            raise ValueError("self.log_gen cannot be None, extract_log first")

        ds = create_parquet_dataset_log(self.log_gen, dataset_folder)

        self.dataset = re_parition_dataset(ds, new_dataset_folder, partition_field)

        return self

    def transform(self):
        """
        Transform data before loading datawarehouse
        """
        if self.dataset is None:
            raise ValueError("self.dataset cannot be None")

        if self.hostname_csv is None:
            raise ValueError("self.hostname_csv cannot be None, use extract_log")

        self.final_table = join_with_hostnames(self.dataset, self.hostname_csv)
        return self

    def load_datawarehouse(self, table_name):
        load_to_duckdb(self.con, self.final_table, table_name)
