"""
Данные для визуализации на карте -
Имя файлов: tmp/{country}_relations.csv (например russia_relations.csv)
Формат: "Main", "Страна",
"Отставание/опережение (дней)",
"Степень уверенности (из 10)",
"longitude", "latitude"
"""


import pandas as pd
import numpy as np
import json

country = "Russia"

columns = ["Country1", "Country2", "Lag", "Degree_of_certainty"]
result_file = f"{country.lower()}_relations.csv"
COORDS = "https://raw.githubusercontent.com/gavinr/world-countries-centroids/master/dist/countries.csv"


def get_graph_data(query_country, similarity_info):
    c = similarity_info[query_country]
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


def remove_positive(df):
    return df[df["Lag"] < 0]


def change_coords(df):
    mapping = {
        "United Kingdom": (53.669806, -2.043880),
        "France": (46.061738, 1.792240),
        "Norway": (65.516408, 13.925926),
        "Netherlands": (52.583765, 5.874290),
        "Denmark": (56.062883058447404, 9.553329909933854),
        "Australia": (-33.7481033657015, 149.2342790558317),
        "Malaysia": (4.040268965452877, 102.0898863270543),
        "Spain": (40.289577069220314, -3.4551207010699447),
        "Canada": (60.17407241207174, -108.4370255907296),

    }
    for country, values in mapping.items():
        df.loc[df["COUNTRYAFF"] == country, ["latitude", "longitude"]] = values

    return df


if __name__ == "__main__":
    with open("tmp/lags.json") as f:
        lags = json.load(f)

    coords = (
        pd.read_csv(COORDS)
        .pipe(change_coords)
        .groupby(by="COUNTRYAFF")
        .agg({"longitude": np.mean, "latitude": np.mean})
    )
    (
        get_graph_data(country, lags)
        .merge(coords, left_on="Country2", right_on="COUNTRYAFF", how="inner")
        .loc[:, columns + ["longitude", "latitude"]]
        .pipe(to_bins)
        .pipe(remove_positive)
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
