import pandas as pd
from sqlalchemy import create_engine
from envparse import env

env.read_envfile()

engine = create_engine(env("DB"))
df = pd.read_csv("tmp/russia_relations.csv", encoding="utf8").rename(
    columns={
        "Country1": "Main",
        "Страна": "Country",
        "Отставание/опережение (дней)": "Lag",
        "Степень уверенности (из 10)": "degree",
    }
)
df.to_sql("countries", con=engine, index=False, if_exists="replace")
