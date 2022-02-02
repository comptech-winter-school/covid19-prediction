import pandas as pd
from sqlalchemy import create_engine
from envparse import env


if __name__ == "__main__":
    env.read_envfile()

    engine = create_engine(env("DB"))
    (
        pd.read_csv("tmp/russia_relations.csv", encoding="utf8")
        .rename(
            columns={
                "Country1": "Main",
                "Страна": "Country",
                "Отставание/опережение (дней)": "Lag",
                "Степень уверенности (из 10)": "degree",
            }
        )
        .to_sql("countries", con=engine, index=False, if_exists="replace")
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
        .to_sql("vaccination", con=engine, index=False, if_exists="replace")
    )
