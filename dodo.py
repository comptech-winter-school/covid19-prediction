#!/usr/bin/env python3


def task_download_data():
    return {
        "actions": ["cd COVID-19 && git pull && git rev-parse HEAD > ../git"],
        "targets": ["COVID-19/.git/refs/heads/master"],
    }


def task_prepare_data():
    return {
        "actions": ["venv/bin/python prepare_data.py"],
        "file_dep": ["COVID-19/.git/refs/heads/master", "prepare_data.py"],
        "targets": ["tmp/data.parquet"],
    }


def task_find_lag():
    return {
        "actions": ["venv/bin/python find_lag.py 0"],
        "file_dep": ["tmp/data.parquet", "find_lag.py"],
        "targets": ["tmp/lags.json", "tmp/cases.pickle"],
    }


def task_find_lag_30():
    return {
        "actions": ["venv/bin/python find_lag.py 30"],
        "file_dep": ["tmp/data.parquet", "find_lag.py"],
        "targets": ["tmp/lags_30.json", "tmp/cases_30.pickle"],
    }


def task_find_lag_60():
    return {
        "actions": ["venv/bin/python find_lag.py 60"],
        "file_dep": ["tmp/data.parquet", "find_lag.py"],
        "targets": ["tmp/lags_60.json", "tmp/cases_60.pickle"],
    }


def task_find_lag_90():
    return {
        "actions": ["venv/bin/python find_lag.py 90"],
        "file_dep": ["tmp/data.parquet", "find_lag.py"],
        "targets": ["tmp/lags_90.json", "tmp/cases_90.pickle"],
    }


def task_create_relations():
    return {
        "actions": ["venv/bin/python create_lags_data.py"],
        "file_dep": ["create_lags_data.py"],
        "targets": ["tmp/russia_relations.csv"],
    }


def task_upload_data():
    return {
        "actions": ["venv/bin/python upload_data.py"],
        "file_dep": ["tmp/russia_relations.csv", "tmp/lags.json", "upload_data.py"],
    }
