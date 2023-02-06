import argparse
from datapipeline import DataPipeline


def run(argv=None):
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-log_dir", help="directory of the log files")
    parser.add_argument("-log_pat", help="log file name pattern")
    parser.add_argument("-hostname_file", help="hostname csv file")
    args = parser.parse_args(argv)

    # data pipeline
    with DataPipeline(type="apache_web_logs", database="apache_web_logs") as p:
        # fmt: off
        p \
        .extract_log(directory=args.log_dir, file_pattern=args.log_pat, hostname_csv=args.hostname_file) \
        .clean_log_record() \
        .load_to_data_lake(re_partition=True, new_dataset_folder="part_log_datalake", partition_field="status") \
        .transform() \
        .load_datawarehouse(table_name="web_logs") 
        # fmt: on


if __name__ == "__main__":
    run()

# TODO: add logging to class and capture status
# TODO: output status on completion
