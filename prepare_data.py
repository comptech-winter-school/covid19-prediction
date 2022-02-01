import os
from pathlib import Path

import pandas as pd

if not Path("tmp").exists():
    os.mkdir("tmp")
assert Path(
    "COVID-19"
).exists(), (
    "необходимо создать ссылку на склонированный репозиторий в папке со скриптом"
)

path = "COVID-19/csse_covid_19_data/csse_covid_19_daily_reports"
all_files = Path(path).glob("*.csv")


def remove_small_countries(df):
    countries = (
        df.groupby("Country_Region").last().query("Confirmed > 100000").index.to_list()
    )
    return df[df.Country_Region.isin(countries)]


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