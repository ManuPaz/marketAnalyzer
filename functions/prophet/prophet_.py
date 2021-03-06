import itertools

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_cross_validation_metric

from config import load_config
from logs import info
from plots import plots_prophet
config = load_config.config()
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
import random
from functions import metricas
def params_prophet(param_grid):
    all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
    n = config["prophet"]["number_combs"]
    all_params_2 = [e for e in all_params if e['changepoint_prior_scale'] != 0.05 or e['seasonality_prior_scale'] != 10]
    params_default = [e for e in all_params if
                      e['changepoint_prior_scale'] == 0.05 and e['seasonality_prior_scale'] == 10]
    indice = random.sample(range(0, len(all_params_2)), n)
    all_params_2 = [e for i, e in enumerate(all_params_2) if i in indice]
    len(params_default), len(all_params_2)
    return params_default, all_params_2
def train_and_predict(df_train, df_test, **params):
    monthly_seasonality = params["monthly_seasonality"]
    monthly_n = params["monthly_N"]
    weekly_n = params["weekly_N"]
    yearly_n = params["yearly_N"]
    use_holidays = params["use_holidays"]

    for key in ["monthly_seasonality", "monthly_N", "yearly_N", "weekly_N", "use_holidays"]:
        if key in params.keys():
            params.pop(key)
    # Creamos un modelo prophet
    model = Prophet(weekly_seasonality=weekly_n, yearly_seasonality=yearly_n, **params)

    # Comprobar si se meten las variables externas
    columnas = list(set(df_train.columns) - set(("ds", "y")))
    for column in columnas:
        model.add_regressor(column)

    if use_holidays:
        model.add_country_holidays(country_name='US')

    if monthly_seasonality:
        model.add_seasonality(name='monthly', period=30.5, fourier_order=monthly_n)

    # Lo entrenamos con nuestro dataframe
    with info.suppress_stdout_stderr():
        model.fit(df_train)

    future = df_test.drop("y", axis=1)
    # Realizar predicciones
    forecast_test = model.predict(future)
    forecast_test["y"] = df_test.reset_index().y
    aux = pd.concat([df_train, df_test], axis=0)
    future = aux.drop("y", axis=1)
    forecast_full_dataframe = model.predict(future)
    forecast_full_dataframe["y"] = aux.reset_index().y
    return model, forecast_test, forecast_full_dataframe

def train(df, **params):
    monthly_seasonality = params["monthly_seasonality"]
    monthly_n = params["monthly_N"]
    weekly_n = params["weekly_N"]
    yearly_n = params["yearly_N"]
    use_holidays = params["use_holidays"]

    for key in ["monthly_seasonality", "monthly_N", "yearly_N", "weekly_N", "use_holidays"]:
        if key in params.keys():
            params.pop(key)
    # Creamos un modelo prophet
    model = Prophet(weekly_seasonality=weekly_n, yearly_seasonality=yearly_n, **params)

    # Comprobar si se meten las variables externas
    columnas = list(set(df.columns) - set(("ds", "y")))
    for column in columnas:
        model.add_regressor(column)

    if use_holidays:
        model.add_country_holidays(country_name='US')

    if monthly_seasonality:
        model.add_seasonality(name='monthly', period=30.5, fourier_order=monthly_n)

    # Lo entrenamos con nuestro dataframe
    with info.suppress_stdout_stderr():
        model.fit(df)

    future = df.drop("y", axis=1)
    # Realizar predicciones
    forecast = model.predict(future)
    forecast["y"] = df.reset_index().y

    return model, forecast
def make_cross_validation(model, period="60 days", horizon='60 days', cutoffs=None):
    with info.suppress_stdout_stderr():
        print(cutoffs)
        if cutoffs is None:
            print("Period {}, horizon {}".format(period, horizon))
            df_cv = cross_validation(model, period=period, horizon=horizon)
        else:
            df_cv = cross_validation(model, cutoffs=cutoffs, horizon=horizon)
        df_p = performance_metrics(df_cv)

        return df_p, df_cv
def report_cross_validation(forecast_full, forecast, df_cv, df_metrics, feature, model):
    """

    :param forecast_full: prophet dataframe with y series also (real values) of train and test predition together
    :param forecast: prophet dataframe only with test predition
    :param df_cv: prophet dataframe from corss_validation
    :param df_metrics:  prophet metrics  dataframe from corss_validation
    :param feature: feature to predict
    :param model: prophet model
    """
    indices = np.unique(df_cv.cutoff)

    # graficos y metricas para cada k-fold
    for i, ind in enumerate(indices):
        dat = df_cv.loc[df_cv.cutoff == ind]
        d1 = forecast_full.loc[forecast_full.ds < ind]
        model.plot_components(pd.concat([d1, dat]))
        plots_prophet.plot_separated(pd.concat([d1, dat]), feature, ind)
        metrics = metricas.all_metricas(dat.yhat, dat.y)
        if i < len(indices) - 1:
            logger.info('prophet_: METRICAS desde fecha {} hasta fecha: {},{}'.format(ind, indices[i + 1], metrics))
        else:
            logger.info('prophet_: METRICAS desde fecha {} hasta fecha: {}, {}'.format(ind, forecast_full.ds.values[-1],
                                                                                       metrics))
    # metricas en cross validation y validation test
    plot_cross_validation_metric(df_cv, metric='mape')
    plot_cross_validation_metric(df_cv, metric='smape')
    plot_cross_validation_metric(df_cv, metric='rmse')
    plt.show()
    logger.info(
        "prophet_: METRICAS medio en cross validation: rmse={}, smape={}, mape={},".format(df_metrics["rmse"].mean(),
                                                                                           df_metrics["smape"].mean(),
                                                                                           df_metrics["mape"].mean()))
    logger.info("prophet_: METRICAS en validation set: {}".format(metricas.all_metricas(forecast.y, forecast.yhat)))
