from datetime import datetime
import logging
import re
from typing import Generator
from uuid import uuid4

import pyarrow as pa

from src.extract_load import (
    get_dataset,
    map_function_to_element,
    read_files,
    write_to_paruqet_from_batch_gen,
)
from src.transform_load import join_with_hostnames


class ApacheDataPipeline:
    def __init__(
        self, database_connection, partition_field=None, output_location="output/"
    ):
        self.hostname_csv = None
        self.output_location = output_location
        self.partition_field = None
        self.log_gen: Generator | None = None
        self.con = database_connection
        self.partition_field = partition_field
        self.schema = pa.schema(
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
        )

    def match_pattern(self, lines):
        """
        match apache log using regex
        """
        line_pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(\S+) (\S+) (\S+)" (\S+) (\S+) "(.*?)" "(.*?)"'
        log_line_pattern = re.compile(line_pattern)
        for line in lines:
            groups = log_line_pattern.match(line)
            if groups:
                matched = groups.groups()
                yield matched
            else:
                with open("errors/incorrect_data.txt", "a") as f:
                    f.write(line)
                yield None

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
        # groups = (log_line_pattern.match(line) for line in lines)  # type: ignore
        # matched = (g.groups() for g in groups if g)

        # creating a key, value pair
        matched = self.match_pattern(lines)
        self.log_gen = (dict(zip(cols, m)) for m in matched if m is not None)
        return self

    def clean_log_record(self):
        """
        clean log
        """
        # self.log_gen cannot be None
        if self.log_gen is None or self.log_gen == {}:
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

    def load_to_data_lake(self):
        """
        load data to data lake as parquet files
        """
        self.partition_field = self.partition_field
        if self.log_gen is None:
            raise ValueError("self.log_gen cannot be None, extract_log first")

        filename = self.output_location + datetime.utcnow().strftime(
            f"%Y-%m-%d-{uuid4()}.parquet"
        )

        write_to_paruqet_from_batch_gen(self.log_gen, filename, self.schema)

        logging.info(
            f"Data is written into data lake, directory is {self.output_location}"
        )
        return self

    def transform_data(self):
        """
        Transform data before loading datawarehouse
        """
        if self.hostname_csv is None:
            raise ValueError("self.hostname_csv cannot be None, use extract_log")

        dataset = get_dataset(self.output_location).to_table()

        self.final_table = join_with_hostnames(dataset, self.hostname_csv)
        logging.info("Data is joined with hostname and ready to load into duckdb")
        return self

    def load_datawarehouse(self):
        final_table = self.final_table
        self.con.execute("CREATE TABLE joined_log_table AS SELECT * FROM final_table")
        logging.info(
            "Transformed data has been loaded into duckdb, and table name is joined_log_table"
        )
        return self
