"""
Данные о похожести стран -
Имя файлов: tmp/lags{_last_n_days}.json (например lags_30.json)
Формат: {"Страна": {"Страна": {"lag": int (отрицательное число), "similarity": float}}}
Данные о заражении -
Имя файлов: tmp/cases{_last_n_days}.pickle (например cases_30.pickle)
Формат: {"Страна": {"filtered_history": pd.Series - отфильтрованные данные без последних last_n_days дней,
            "filtered_validate": pd.Series - отфильтрованные данные, полследние last_n_days дней,
            "validate": pd.Series - реальные исторические данные, последние last_n_days дней,
            "history": pd.Series - реальные исторические данные без последних last_n_days дней}}
"""

import argparse
import itertools
import json
import os
import pickle
from collections import defaultdict
from pathlib import Path

import pandas as pd
from scipy.signal import savgol_filter


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get lags for countries.")
    parser.add_argument("predict_days", default=0, type=int, help="predict_days")

    args = parser.parse_args()
    predict_days = args.predict_days

    if not Path("tmp").exists():
        os.mkdir("tmp")

    df = pd.read_parquet("tmp/cov19.parquet")
    countries = df.Country_Region.unique().tolist()
    cache = {}

    def get_new_cases(d, c):
        """Взятие новых кейсов из датафрейма d для страны c и их сглаживание фильтром"""
        new_v = d[d["Country_Region"] == c]["Confirmed"]
        real = (new_v - new_v.shift(1)).dropna()

        f = savgol_filter(real.to_numpy(dtype=float), 51, 2)
        after_filter_full = pd.Series(f, index=new_v.index[:-1])

        if predict_days > 1:
            res = {
                "filtered_history": after_filter_full.iloc[:-predict_days].copy(),
                "filtered_validate": after_filter_full.iloc[-predict_days:].copy(),
                "validate": real.iloc[-predict_days:].copy(),
                "history": real.iloc[:-predict_days].copy(),
            }
        else:
            res = {
                "filtered_history": after_filter_full,
                "filtered_validate": None,
                "validate": None,
                "history": real,
            }

        return res

    new_cases = {c: get_new_cases(df, c) for c in countries}

    dl=pd.read_parquet('tmp/owid-covid-data.parquet',
    delimiter=',') 


    pairs = list(itertools.combinations(countries, 2))

    result = defaultdict(dict)
    threshold = 0.4
    for country_1, country_2 in pairs:
        cases_1, cases_2 = new_cases[country_1], new_cases[country_2]
        c1, c2 = cases_1["filtered_history"], cases_2["filtered_history"]
        values = []

        dr2 = df.loc[df['Country_Region'] == country_2, 'PopTotal']
        c2_new_test = dl.loc[df['Country_Region'] == country_2, 'new_tests']
        c2_total_vac = dl.loc[df['Country_Region'] == country_2, 'total_vaccinations']


        coef = (c2_new_test/ dr2 *1000) + (  c2_total_vac / dr2 * 1000)

        for i in range(-91, 1, 1):
            values.append((c1.shift(i).corr(c2) * coef, i))
        similarity, lag = max(values, key=lambda x: x[0])
        if lag < 0:
            country_2, country_1 = country_1, country_2
        if (similarity > threshold) and (lag not in [-90, 30, 0]):
            result[country_2][country_1] = {"similarity": similarity, "lag": lag}

    with open(
        f"tmp/lags{'' if predict_days == 0 else f'_{predict_days}'}.json", "w"
    ) as f:
        json.dump(result, f, sort_keys=True, indent="  ")

    with open(
        f"tmp/cases{'' if predict_days == 0 else f'_{predict_days}'}.pickle", "wb"
    ) as f:
        pickle.dump(new_cases, f)