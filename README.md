# DATE ENGINEERING 101
## Star Talk @ Gapstars 2023 Feb

This repository contains codes and relevant links that shared during the session.

# Installation

Initial setup
```bash
# virtual env setup
python3 -m venv venv # create a virtual env
source venv/bin/activate # activate linux or unix system

# package installation
pip install -e . # make sure pip is available
```

Download the apache web server logs and client hostname (link provided down below), original file has 10 million records
you can split the file into desired amount of lines, (prefer 2 million lines)

This command can split file into chunks by giving number of lines (linux)
```bash
split file_name -l 2000000 output_file

```
Create directory called "data" and place the log file
Create directory called "output/apache_logs" for output
```bash
# folder structure 
├── data
├── main.py
├── output/apache_logs
├── pyproject.toml
├── README.md
└── src
```

Run this to execute the pipeline

```bash
python3 -m main -log_dir=data -log_pat=access.log -hostname_file=data/client_hostname.csv
```


# Resources
* Apache web server logs & client hostname- [link](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs?select=access.log)
* Pyarrow - [link](https://arrow.apache.org/docs/python/index.html)
* Duckdb - [link](https://duckdb.org/)
* Python generator advance application - [link](http://www.dabeaz.com/generators/)
* Fundamentals of data engineering - [link](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/)
