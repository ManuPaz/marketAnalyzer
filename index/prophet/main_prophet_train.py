"""
Configuration and logic dependencies:
    uses config.yaml : to prophet configuration
"""
import os
print(os.getcwd())
os.chdir("../../")
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzig_data')
import pandas as pd
from config import load_config
from functions.prophet import prophet_, logs_prophet
from plots import plots_prophet
from functions.prepare_data import dataframe_equities_with_options
from datetime import timedelta
from functions.estacionaridad import estacionaridadYCointegracion
from functions.estacionaridad import logs_estacionaridad
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)
if __name__ == '__main__':
    config = load_config.config()

    data, feature, df_train, df_test, fecha_sep = dataframe_equities_with_options.get_dataframe_default()

    print(data.columns)
    # tamano minimo de entrenamiento (usado para los cutoffs)
    min_tam_train = config["series_temporales"]["min_tam_train"]
    period = config["prophet"]["period"]
    cutoffs = pd.to_datetime(config["prophet"]["cutoffs"])
    horizon = config["prophet"]["horizon"]
    horizont_int = int(horizon.split(" ")[0])

    params_default = config["prophet"]["params_default"]

    if config["prophet"]["use_cutoffs"]:
        cutoffs = [e for e in cutoffs if
                   e > df_train.index[min_tam_train] and e < df_train.index[-1] - timedelta(days=horizont_int)]
        logger.info("main_prophet_train: {}".format(cutoffs))

    # plot de la variable a predecir
    plots_prophet.plot_variable_to_predict(data, columnas=["y"], title=feature)

    # estacionaridad
    resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data["y"])

    # logs of results
    logs_estacionaridad.log_stationarity_coef(resultadosEst)

    # creacion del modelo
    model, forecast, forecast_full = prophet_.train_and_predict(df_train, df_test, **params_default)

    # Realizar predicciones en train
    future_train = df_train.drop("y", axis=1)
    forecast_train = model.predict(future_train)
    forecast_train["y"] = df_train.reset_index()["y"]

    # logs of metrics
    logs_prophet.logs_metricas_prophet(forecast_train)

    # graficos para ver los componentes y las prediccines en train
    model.plot_components(forecast_full)
    plots_prophet.plot_real_predicted_values(model, forecast_full, df_test, title='Forecast')

    # grafico de validation
    plots_prophet.plot_separated(forecast_full, "Forecast vs real", fecha_sep)

    # cros validation con cutoffs o horizon
    if config["series_temporales"]["cross_validation"]:
        if not config["prophet"]["use_cutoffs"]:
            df_metrics, df_cv = prophet_.make_cross_validation(model, period=period, horizon=horizon)
        else:
            df_metrics, df_cv = prophet_.make_cross_validation(model, period=period, horizon=horizon, cutoffs=cutoffs)
        prophet_.report_cross_validation(forecast_full, forecast, df_cv, df_metrics, feature, model)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
