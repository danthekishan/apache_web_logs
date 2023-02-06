from pyarrow import csv


def join_with_hostnames(log_ds, hostname_csv):
    host_name_table = csv.read_csv(hostname_csv)
    joined_log = (
        log_ds.to_table()
        .join(host_name_table, keys="host", right_keys="client")
        .select(
            ["host", "datetime", "method", "request", "status", "bytes", "hostname"]
        )
    )
    return joined_log


def load_to_duckdb(con, arrow_table, table_name):
    con.execute("CREATE TABLE {} AS SELECT * FROM {}".format(arrow_table, table_name))
