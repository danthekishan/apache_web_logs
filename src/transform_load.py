import logging
import pyarrow as pa
from pyarrow import csv


def join_with_hostnames(log_table, hostname_csv):
    host_name_table = csv.read_csv(hostname_csv)
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.info(
        "Allocated memory by pyarrow {} after reading hostname csv".format(ram)
    )
    joined_log = log_table.join(
        host_name_table, keys="host", right_keys="client"
    ).select(["host", "datetime", "method", "request", "status", "bytes", "hostname"])
    ram = "RSS (RAM): {}MB".format(pa.total_allocated_bytes() >> 20)
    logging.info(
        "Allocated memory by pyarrow {} after joining hostname with arrow table".format(
            ram
        )
    )
    return joined_log
