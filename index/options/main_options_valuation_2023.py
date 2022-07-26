import os
os.chdir("../../")
from utils.database import database_functions,bd_handler
import logging.config
import math
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
from functions.derivatives_valuation import options
date_init="2022-01-01"
date_end=dt.datetime.today()
event="ibex 35"
event_benchmark="s&p 500"
tipo="index"
freq="B"
annual_rate=0.03
stocks={"MC_ACS":{"dividend":0.5,"strike":22,"date_end":"2023-06-16"},
        "MC_MTS":{"dividend":0.2,"strike":22,"date_end":"2023-03-17"},
        "MC_ACX":{"dividend":0.5,"strike":8.75,"date_end":"2023-06-16"},
        "MC_BKT":{"dividend":0.13,"strike":4.8,"date_end":"2023-06-16"},
        "MC_CABK":{"dividend":0.15,"strike":2.9,"date_end":"2023-06-16"},
        "MC_NTGY":{"dividend":0.5,"strike":26,"date_end":"2023-09-16"}}

if __name__ == "__main__":
    stocks_name = list(stocks.keys())
    bd=bd_handler.bd_handler("stocks")
    bd_market_data=bd_handler.bd_handler("market_data")
    precios = time_measure.time_measure(load_dataframe.load_dataframe_bd_csv,
                                        ["data/processed/dataframes/options_valuation.csv",
                                         database_functions.obtener_multiples_series,
                                         ["precios", "B", bd, *stocks_name]])
    precios.set_index("fecha", inplace=True)
    precios.index = pd.to_datetime(precios.index)
    precios
    index_1 = database_functions.get_series_activos_diferentes_de_acciones(None, event, tipo, freq, event,
                                                                         bd_market_data)
    index_benchmark = database_functions.get_series_activos_diferentes_de_acciones(None, event_benchmark, tipo, freq, event_benchmark,
                                                                           bd_market_data)
    date_init=dt.datetime.strptime(date_init,"%Y-%m-%d")
    index=pd.MultiIndex
    precios=precios.loc[date_init:date_end]
    precios_aux=precios.copy()
    precios_aux[event] = index_1
    precios_aux[event_benchmark] = index_benchmark
    cor=precios_aux.corr()
    precios = precios.resample("W").last()
    for stock in precios.columns:
        t=(dt.datetime.strptime(stocks[stock]["date_end"], "%Y-%m-%d") - date_end).days/365
        volatility= precios[stock].pct_change().std()*math.sqrt(52)
        underliying=precios.iloc[-1][stock]
        dividend=stocks[stock]["dividend"]
        daily_rate= (1 + annual_rate) ** (1 / t) - 1

        price=options.put_prize(stocks[stock]["strike"], underliying,0, annual_rate, t, volatility)
        print("Stock %s, T %s, volatility %s, underliying %s, dividend %s, rate %s%%, put theorical price %s"%(stock,t,round(volatility,3),underliying,dividend,annual_rate*100,round(price,3)))

