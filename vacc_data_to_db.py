import pandas as pd
from sqlalchemy import create_engine
from envparse import env

env.read_envfile()

engine = create_engine(env("DB"))
df = pd.read_csv("https://covid.ourworldindata.org/data/owid-covid-data.csv")

loc_to_drop = ['World', 'High income', 'European Union',
               'Europe', 'Upper middle income',
               'Asia', 'Lower middle income', 'Low income']
(df
 .drop(['iso_code', 'continent'], axis=1)
 .drop(df.loc[df['location'].isin(loc_to_drop), :].index, axis=0)
 .to_sql("vaccination", con=engine, index=False, if_exists="replace")
)
