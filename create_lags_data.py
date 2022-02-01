import pandas as pd
import numpy as np
import json

columns = ["Country1", "Country2", "Lag", "Degree_of_certainty"]
country = "Russia"
result_file = "russia_relations.csv"
COORDS = "https://raw.githubusercontent.com/gavinr/world-countries-centroids/master/dist/countries.csv"


def get_graph_data(query_country, lags):
    c = lags[query_country]
    buff_lines = []
    for k, v in c.items():
        buff_lines.append((query_country, k, v["lag"], v["similarity"]))
    res = pd.DataFrame(buff_lines, columns=columns)
    return res


def to_bins(df):
    df["Degree_of_certainty"] = pd.cut(
        df["Degree_of_certainty"], np.arange(0, 1.1, 0.1), labels=list(range(1, 11, 1))
    )
    return df


with open("tmp/lags.json") as f:
    lags = json.load(f)

(
    get_graph_data(country, lags)
    .merge(pd.read_csv(COORDS), left_on="Country2", right_on="COUNTRYAFF", how="inner")
    .loc[:, columns + ["longitude", "latitude"]]
    .pipe(to_bins)
    .rename(
        columns={
            "Country1": "Main",
            "Country2": "Страна",
            "Lag": "Отставание/опережение (дней)",
            "Degree_of_certainty": "Степень уверенности (из 10)",
        }
    )
    .to_csv(f"tmp/{result_file}")
)
