import logging
from datetime import datetime
import re
from typing import Generator
from extract_load import (
    read_files,
    map_function_to_element,
    create_arrow_table,
    write_arrow_table_to_partitioned_parquet,
    get_dataset,
)
from transform_load import join_with_hostnames


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

        lines = read_files(directory, file_pattern)

        # spliting by regex
        groups = (log_line_pattern.match(line) for line in lines)  # type: ignore
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
        partition_field=None,
    ):
        """
        load data to data lake as parquet files
        """
        if self.log_gen is None:
            raise ValueError("self.log_gen cannot be None, extract_log first")

        write_arrow_table_to_partitioned_parquet(
            create_arrow_table(self.log_gen), dataset_folder, partition_field
        )

        self.dateset = get_dataset(dataset_folder)
        logging.INFO(
            "Data is written into data lake, directory is {}".format(dataset_folder)
        )

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
        logging.INFO("Data is joined with hostname and ready to load into duckdb")
        return self

    def load_datawarehouse(self):
        self.con.execute(
            "CREATE TABLE joined_log_table AS SELECT * FROM self.final_table"
        )
        logging.INFO(
            "Transformed data has been loaded into duckdb, and table name is joined_log_table"
        )
        return self
