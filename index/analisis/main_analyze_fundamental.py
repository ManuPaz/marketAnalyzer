import os
os.chdir("../../")
from utils.database import database_functions, bd_handler
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
logger_analyzer = logging.getLogger('analyzing_data')
EXCHANGE = "US"
INDICE = None
UPDATE = False
from utils.time_measure import time_measure
from utils.improve_performance import load_dataframe
import pandas as pd
import datetime as dt
from functions.fundamental import fundamental_analisis
from utils.database import stocks_queries_string
date_init = "1990-01-01"
date_end = dt.datetime.today()
event_benchmark = "s&p 500"
tipo = "index"
freq = "B"
annual_rate = 0.03
stocks = ["US_T", "US_TMUS", "US_VZ"]
columns = ["netDebt", "totalRevenue", "ebitda", "ebit", "netIncome", "totalAssets", "totalStockholderEquity",
           "commonStockSharesOutstanding"]
if __name__ == "__main__":
    bd = bd_handler.bd_handler("stocks")
    bd_market_data = bd_handler.bd_handler("market_data")
    precios = time_measure.time_measure(load_dataframe.load_dataframe_bd_csv,
                                        ["data/processed/dataframes/comunications.csv",
                                         database_functions.obtener_multiples_series,
                                         ["precios", "B", bd, *stocks]])
    precios.set_index("fecha", inplace=True)
    precios.index = pd.to_datetime(precios.index)
    precios
    index_benchmark = database_functions.get_series_activos_diferentes_de_acciones(None, event_benchmark, tipo, freq,
                                                                                   event_benchmark,
                                                                                   bd_market_data)
    date_init = dt.datetime.strptime(date_init, "%Y-%m-%d")
    index = pd.MultiIndex
    precios = precios.loc[date_init:date_end]
    precios_aux = precios.copy()
    precios_aux[event_benchmark] = index_benchmark
    for stock in stocks:

        fundamental = bd.execute_query_dataframe(stocks_queries_string.FUNDAMENTAL_RAW.format(EXCHANGE),
                                                 (stock.split("_")[1],))
        fundamental = fundamental.dropna(how="all").dropna(how="all", axis=1)
        fundamental["fecha"] = pd.to_datetime(fundamental["fecha"])
        fundamental.drop(["exchange", "stock"], axis=1, inplace=True)
        fundamental.set_index("fecha", inplace=True)
        fundamental = fundamental.loc[:, columns]
        fundamental = fundamental.dropna(how="all", axis=1)
        fundamental_analisis.fundamental_correlation_with_prizes(fundamental, precios.loc[:, [stock]], freq="Y",stock=stock)
