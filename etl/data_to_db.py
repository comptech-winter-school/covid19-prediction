import pandas as pd
from sqlalchemy import create_engine
from envparse import env


if __name__ == "__main__":
    env.read_envfile()

    engine = create_engine(env("DB"))
    (
        pd.read_csv("tmp/relations.csv", encoding="utf8")
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

    (
        pd.read_csv("tmp/people_structure.csv")
        .to_sql("people_structure", con=engine, index=False, if_exists="replace")
    )
