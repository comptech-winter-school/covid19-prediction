#!/usr/bin/env python3

def task_download_data():
    return {
        'actions': ['cd COVID-19 && git pull && git rev-parse HEAD > ../git'],
        'targets': ['COVID-19/.git/refs/heads/master']
    }


def task_prepare_data():
    return {
        'actions': ["papermill prepare_data.ipynb out/prepare_data.ipynb"],
        'file_dep': ['COVID-19/.git/refs/heads/master', 'prepare_data.ipynb'],
        'targets': ['tmp/data.parquet']
    }

def task_find_lag():
    return {
        'actions': ["papermill find_lag.ipynb out/find_lag.ipynb"],
        'file_dep': ['tmp/data.parquet', 'find_lag.ipynb', 'consts.py'],
        'targets': ['tmp/lags.json']
    }

# def task_predict():
#     return {
#         'actions': ["papermill predict.ipynb out/predict.ipynb"],
#         'file_dep': ['tmp/data.parquet', 'predict.ipynb', 'consts.py'],
#         'targets': ['tmp/lags.json']
#     }
