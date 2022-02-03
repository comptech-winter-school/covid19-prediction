"""
Предобработанные данные из репозитория https://github.com/CSSEGISandData/COVID-19
Имя файла: tmp/data.parquet
Формат данных:
"Last_Update" - дата, является индексом,
"Country_Region" - страны, в которых заражений больше 100 тысяч,
"Confirmed" - количество подтвержденных случаев COVID на момент Last_Update

Предобработанные данные https://covid.ourworldindata.org/data/owid-covid-data.csv
Имя файла: tmp/people_structure.csv
Формат данных: см. в репозитории https://github.com/owid/covid-19-data/tree/master/public/data
"""


import os
from pathlib import Path

import pandas as pd


def remove_small_countries(df):
    countries = (
        df.groupby("Country_Region").last().query("Confirmed > 100000").index.to_list()
    )
    return df[df.Country_Region.isin(countries)]


if __name__ == "__main__":
    if not Path("../tmp").exists():
        os.mkdir("../tmp")

    assert Path(
        "../COVID-19"
    ).exists(), (
        "необходимо создать ссылку на склонированный репозиторий в папке со скриптом"
    )

    path = "../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports"
    all_files = Path(path).glob("*.csv")

    df = pd.concat(map(pd.read_csv, all_files))
    (
        df.assign(Last_Update=pd.to_datetime(df["Last_Update"]).dt.date)
        .loc[:, ["Country_Region", "Last_Update", "Confirmed"]]
        .dropna()
        .groupby(["Country_Region", "Last_Update"])
        .sum()
        .reset_index()
        .set_index("Last_Update")
        .sort_index()
        .pipe(remove_small_countries)
        .to_parquet("tmp/data.parquet")
    )

    df = pd.read_csv("https://covid.ourworldindata.org/data/owid-covid-data.csv")

    loc_to_drop = [
        "World",
        "High income",
        "European Union",
        "Europe",
        "Upper middle income",
        "Asia",
        "Lower middle income",
        "Low income",
    ]

    (
        df.drop(["iso_code", "continent"], axis=1)
        .drop(df.loc[df["location"].isin(loc_to_drop), :].index, axis=0)
        .to_csv("tmp/people_structure.csv")
    )
