import logging
from time import perf_counter
from datetime import datetime
from apache_datapipeline import ApacheDataPipeline
import duckdb


class DataPipeline:
    def __init__(self, pipe_type, database, dataset_location, partition_field):
        self.pipe_type = pipe_type
        self.database = database
        self.dataset_location = dataset_location
        self.time_taken_min = None
        self.partition_field = partition_field

    def __enter__(self):
        _start = perf_counter()
        logging.info(
            "DataPipeline is started at {}".format(datetime.utcnow().isoformat())
        )
        self.con = duckdb.connect(database=self.database, read_only=False)
        if self.pipe_type == "apache_web_log":
            self.datapipeline = ApacheDataPipeline(
                self.con, self.dataset_location, self.partition_field
            )
        else:
            logging.error(
                "Incorrect data pipeline type has been passed {}".format(self.pipe_type)
            )
            raise ValueError("Incorrect datapipeline type")
        _end = perf_counter()
        self.time_taken = (_end - _start) / 60
        return self.datapipeline

    def __exit__(self, *args, **kwargs):
        logging.info(
            "DataPipeline is exited took {} minutes".format(self.time_taken_min)
        )
        self.con.close()