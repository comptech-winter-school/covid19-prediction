from prophet import Prophet
import pandas as pd
import httpx
import numpy as np
import json


def get_train_data(countries, source_country, cases, field):
    df = pd.DataFrame(index=cases[cases['location'] == source_country]["date"],
                      columns=['y', 'ds'] + [i for i, _ in countries])
    df['y'] = cases[cases['location'] == source_country].reset_index().set_index("date")[field]
    df['ds'] = cases[cases['location'] == source_country].reset_index().set_index("date").index
    c = cases["location"].unique()

    for country, info in countries:
        if country in c:
            df[country] = cases[cases["location"] == country][field]
        else:
            del df[country]
    return df.fillna(method='ffill').dropna()


def get_df_future(days_predict, m, countries, cases, field, train_last):
    d = pd.date_range(train_last, periods=days_predict + 1, closed='right')
    future = pd.DataFrame()
    future["ds"] = d

    g = cases['location'].unique()

    for country, info in countries:
        if country in g:
            future[country] = cases[cases['location'] == country][field].iloc[
                              -days_predict + info["lag"]: info["lag"]].values

    return future.fillna(method='ffill')


def get_predict(days_predict, source_country, cases, lags) -> np.array:
    countries = sorted(filter(lambda x: -x[1]["lag"] > days_predict, lags[source_country].items()),
                       key=lambda x: x[1]["similarity"])[-2:]
    field = 'new_cases_smoothed'
    train = get_train_data(countries, source_country, cases, field)

    m = Prophet(
        daily_seasonality=False,
        yearly_seasonality=True,
        weekly_seasonality=True,
        changepoint_prior_scale=0.8,
        seasonality_mode='multiplicative'
    )
    for i in train.columns:
        if i != 'ds' and i != 'y':
            m.add_regressor(i, mode="multiplicative")
    m.fit(train)
    future = get_df_future(days_predict, m, countries, cases, field, train.index[-1])
    forecast = m.predict(future)

    return forecast[['ds', 'yhat']]


if __name__ == "__main__":
    with open("people_structure.csv", 'wb') as f:
        f.write(httpx.get("https://storage.yandexcloud.net/covid-19/people_structure.csv").content)

    df_sec = pd.read_csv("people_structure.csv")
    lags = json.loads(httpx.get("https://storage.yandexcloud.net/covid-19/lags.json").content)
    days_predict = 30

    index = None
    df = pd.DataFrame()
    for country in lags.keys():
        predict = get_predict(days_predict, country, df_sec, lags)
        if index is None:
            index = predict["ds"]

        d = pd.DataFrame()
        d["Date"] = index
        d["Country"] = country
        d["Prophet"] = predict["yhat"].values

        # Здесь добавляй в Linear, Mean, Holt предикты

        #####

        df = pd.concat([df, d], ignore_index=True)

    df.to_csv("tmp/predict.csv")

