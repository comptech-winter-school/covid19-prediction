from prophet import Prophet
import pandas as pd
import httpx
import numpy as np
import json
import pickle
from datetime import date, timedelta, datetime
from statsmodels.tsa.holtwinters import ExponentialSmoothing as HWES
import statsmodels.api as sm

def get_cases(days):
  data = httpx.get(f"https://storage.yandexcloud.net/covid-19/cases{\
      ('_' + str(days)) if days != 0 else ''}.pickle").content
  return pickle.loads(data)

# ================================PROPHET================================
# \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def get_train_data(countries, source_country, cases, field):
    df = pd.DataFrame(index=cases[cases['location'] == source_country]["date"],
                      columns=['y', 'ds'] + [i for i, _ in countries])
    df['y'] = cases[cases['location'] == source_country].reset_index().set_index("date")[field]
    df['ds'] = cases[cases['location'] == source_country].reset_index().set_index("date").index
    c = cases["location"].unique()
    for country, info in countries:
        if country in c:
            df[country] = cases[cases["location"] == country].reset_index().set_index("date")[field].shift(info["lag"])
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
    """
    Возвращает предикт и модель Prophet
    """
    countries = sorted(filter(lambda x: -x[1]["lag"] > days_predict, lags[source_country].items()),
                       key=lambda x: x[1]["similarity"])[-2:]
    field = 'new_cases_smoothed'
    train = get_train_data(countries, source_country, cases, field)

    if len(train) == 0:
        return None

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
    return forecast[["ds", 'yhat']]
# ///////////////////////////////////////////////////////////////////////
# =======================================================================


# =================================MEAN==================================
# \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
  
def get_predict_mean(country, n_days, need_predict, cases_all, use_filter=True, pred_from=None):

    field_name = ("filtered_" if use_filter else "") + "history"
    threshold = date(year=2021, month=1, day=1)

    just_cases = cases_all[country][field_name]
    if pred_from:
        mean_value = np.mean(just_cases[pred_from - n_days : pred_from].values)
    else:
        mean_value = np.mean(just_cases[- n_days : ].values)

    predict = np.asarray([mean_value for i in range(0, need_predict)])

    return predict

# ///////////////////////////////////////////////////////////////////////
# =======================================================================

# ================================LINEAR=================================
# \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def get_columns(country, top_3_countries, num_features):
  first = [f"{country}_{i}" for i in range(1, 31)]
  second = []
  for c, _ in top_3_countries:
    for i in range(1, num_features + 1):  
      second.append(f"{c}_{i}")
  second.append('y')
  return first + second


def get_predict_linear(country, lags, need_predict, cases_all, use_filter=True, num_features=30):

  field_name = ("filtered_" if use_filter else "") + "history"
  threshold = date(year=2021, month=1, day=1)

  # PREPARE TRAIN DATA
  a = cases_all[country][field_name] #случаи по стране cглаженные или нет
  a = a[a.index >= threshold] #отсечение - берем даты после 01.01.2021
  df_list = []
  top_3_countries = [
    i for i in sorted(lags[country].items(),
                        key=lambda x: x[1]["similarity"]
                      ) if -i[1]["lag"] > need_predict
    ][-3:] #топ-3 страны с максимальной корреляцией

  for window in range(num_features, (a.index[-1] - threshold).days): # window проходит значения от кол-ва пр-ков до кол-ва дней в рассм. cases
    this_row = list(a[window - num_features : window].values)
    y = a[window]

    for country_similar, info in top_3_countries:

      b = cases_all[country_similar][field_name]
      this_row += list(b[b.index > threshold - timedelta(days=-info["lag"])]
                       [window - num_features : window].values)

    this_row.append(y)
    df_list.append(this_row)

  df = pd.DataFrame(df_list, columns=get_columns(country, top_3_countries, num_features))

  # FIT

  X = sm.add_constant(df.drop('y', axis=1))
  y = df['y']
  model = sm.OLS(y, X).fit()

  # PREDICT

  predicted = []

  for i in range(0, need_predict):

    if i >= num_features:
      p = [1.0] + predicted[-num_features:]
    else:
      p = [1.0] + list(cases_all["Russia"][field_name].iloc[-num_features + i : ].values) + predicted

    for c, info in top_3_countries:

      from_ind = -num_features + info["lag"] + i
      to_ind = info["lag"] + i
      p += list(cases_all[c][field_name].iloc[from_ind : to_ind])
      
    predicted.append(model.predict(p)[0])
  
  return np.array(predicted)
# ///////////////////////////////////////////////////////////////////////
# =======================================================================

# =============================HOLT-WINTERS==============================
# \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def hwes_predict(data, country, days):
 
    # create time series for country
    df = data[data['location'] == country]
    ts = df.set_index(pd.DatetimeIndex(df['date'])).asfreq('D').total_cases
  
    # split the time series
    train = ts.iloc[:-days]

    # fit the model
    model = HWES(train, seasonal_periods=4, trend='mul', seasonal='add')
    fitted = model.fit()

    predict = fitted.forecast(days).values

    return predict
# ///////////////////////////////////////////////////////////////////////
# =======================================================================

# ================================SARIMA=================================
# \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

# ///////////////////////////////////////////////////////////////////////
# =======================================================================

if __name__ == "__main__":
    with open("people_structure.csv", 'wb') as f:
        f.write(httpx.get("https://storage.yandexcloud.net/covid-19/people_structure.csv").content)

    df_sec = pd.read_csv("people_structure.csv")
    lags = json.loads(httpx.get("https://storage.yandexcloud.net/covid-19/lags.json").content)
    cases = get_cases(0)
    days_predict = 30

    index = None
    df = pd.DataFrame()
    for country in lags.keys():
        predict = get_predict(days_predict, country, df_sec, lags)

        if index is None:
            index = predict["ds"] if predict else None

        d = pd.DataFrame()
        d["Date"] = index
        d["Country"] = country

        d["Mean_7"] = get_predict_mean(country, 7, days_predict, cases, use_filter=True)
        d["Linear"] = get_predict_linear(country, lags, days_predict, cases)
        d["Holt-Winters"] = hwes_predict(df_sec, country, days_predict)
        d["Prophet"] = predict["yhat"].values if predict is not None else 0

        df = pd.concat([df, d], ignore_index=True)

    df.to_csv("tmp/predict.csv")