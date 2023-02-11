import argparse
from src.datapipeline import DataPipeline
import logging


def run(argv=None):
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-log_dir", help="directory of the log files")
    parser.add_argument("-log_pat", help="log file name pattern")
    parser.add_argument("-hostname_file", help="hostname csv file")
    args = parser.parse_args(argv)

    # data pipeline
    with DataPipeline(
        pipe_type="apache_web_log",
        database="apache_web_logs",
        dataset_location="apache_logs/",
        partition_field=["status"],
    ) as p:
        # fmt: off
        p \
        .extract_log(directory=args.log_dir, file_pattern=args.log_pat, hostname_csv=args.hostname_file) \
        .clean_log_record() \
        .load_to_data_lake()\
        .transform_data() \
        .load_datawarehouse()

        # fmt: on


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

# TODO: add logging to class and capture status
# TODO: output status on completion
