import itertools
import json
import os
import pickle
from collections import defaultdict
from pathlib import Path

import pandas as pd
from scipy.signal import savgol_filter

from consts import predict_days

if not Path('tmp').exists():
    os.mkdir('tmp')


df = pd.read_parquet("tmp/data.parquet")
countries = df.Country_Region.unique().tolist()
cache = {}


def get_new_cases(d, c):
    """Взятие новых кейсов из датафрейма d для страны c и их сглаживание фильтром"""
    c1 = cache.get(c)
    if not c1 is None:
        return c1

    new_v = d[d["Country_Region"] == c]["Confirmed"]
    c1 = (new_v - new_v.shift(1)).dropna()
    try:
        f = savgol_filter(c1.to_numpy(), 51, 2)
        c1 = pd.Series(f, index=new_v.index[:-1])
        if predict_days > 1:
            c2 = c1.iloc[:-predict_days]
        cache[c] = c2
        return c2, c1.iloc[-predict_days:]
    except ValueError as e:
        return pd.Series(), pd.Series()


new_cases = {c: get_new_cases(df, c) for c in countries}

pairs = list(itertools.combinations(countries, 2))

result = defaultdict(dict)
threshold = 0.4
for c_one, c_two in pairs:
    c1, _ = new_cases[c_one]
    c2, _ = new_cases[c_two]
    values = []
    for i in range(-30, 30, 1):
        values.append((c1.shift(i).corr(c2), i))
    similarity, lag = max(values, key=lambda x: x[0])
    if lag < 0:
        c_two, c_one = c_one, c_two
    if (similarity > threshold) and (lag not in [-30, 30, 0]):
        result[c_two][c_one] = {'similarity': similarity, 'lag': lag}

with open('tmp/lags.json', 'w') as f:
    json.dump(result, f, sort_keys=True, indent='  ')

with open('tmp/cases.json', 'wb') as f:
    pickle.dump(new_cases, f)
