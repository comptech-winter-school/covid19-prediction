#!/usr/bin/env python3

def task_download_data():
    return {
        'actions': ['cd COVID-19 && git pull && git rev-parse HEAD > ../git'],
        'targets': ['COVID-19/.git/refs/heads/master']
    }


def task_prepare_data():
    return {
        'actions': ["venv/bin/python prepare_data.py"],
        'file_dep': ['COVID-19/.git/refs/heads/master', 'prepare_data.py'],
        'targets': ['tmp/data.parquet']
    }


def task_find_lag():
    return {
        'actions': ["venv/bin/python find_lag.py"],
        'file_dep': ['tmp/data.parquet', 'find_lag.py', 'consts.py'],
        'targets': ['tmp/lags.json']
    }
