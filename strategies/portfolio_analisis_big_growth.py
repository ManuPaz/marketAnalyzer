

import os
os.chdir("../")
from config import load_config
config=load_config.config()
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
import datetime as dt
from utils.database import bd_handler, database_functions
from plots import other_plots

other_plots
import pandas as pd
import numpy as np
from utils.time_measure import time_measure
from utils.improve_performance import load_dataframe
from statsmodels.stats.diagnostic import lilliefors
import warnings
warnings.filterwarnings("ignore")
logging.getLogger('matplotlib').setLevel(logging.ERROR)
check_normality=False
import datetime as dt
from utils import transform_functions
data_dir="data/processe/dataframes/"
EXCHANGE="US"
if __name__=="__main__":

    expansion_period=transform_functions.time_array_sto_str(["2021-01-01", "2021-01-12"], "%Y-%m-%d")
    contraction_period = transform_functions.time_array_sto_str(["2022-01-01", "2022-07-01"], "%Y-%m-%d")

    bd = bd_handler.bd_handler("stocks")
    fecha_ini = dt.datetime(2017, 3, 1)
    fecha_end = dt.datetime(2022, 1, 1)
    stocks_ciberseguridad = ["US_SPLK","US_RPD","US_OKTA","US_CSCO","US_S","US_NET","US_PANW","US_CRWD","US_FTNT","US_ZS"
                             "US_AKAM","US_DDOG"]
    stocks_bigcap=["US_MSFT","US_META","US_NFLX","US_NVDA","US_TSLA","US_QCOM","US_PYPL"]
    stocks_cloud=["US_CRM","US_ADBE","US_TTD","US_DOCN","US_ZM"]
    stocks_quant_computing=[]
    stocks_defensive=[]
    stocks_energy=[]
    stocks_reneable=[]
    stocks_name =stocks_ciberseguridad
    precios = time_measure.time_measure(load_dataframe.load_dataframe_bd_csv,
                                        ["data/processed/dataframes/cibersecurity.csv",
                                         database_functions.obtener_multiples_series,
                                         ["precios", "B", bd, *stocks_name]])
    log_returns = np.log(precios)
    log_returns = log_returns.diff()
    log_returns_expansion=log_returns.loc[(log_returns.index>=expansion_period[0])&(log_returns.index<=expansion_period[1])]
    log_returns_contraction = log_returns.loc[(log_returns.index >= contraction_period[0])&(log_returns.index <=contraction_period[1])]


    fundamental_columns = ["totalOperatingExpenses", "netIncome", "totalRevenue", "grossProfit", "totalAssets",
                           "totalCurrentLiabilities", "netDebt"]
    fundamental = database_functions.obtener_fundamenal_varios_stocks_multiples_columns("Q", stocks_name,
                                                                                        fundamental_columns, None, bd)
    if fundamental.index.name != "fecha":
        fundamental.set_index("fecha", inplace=True)
    fundamental.index = fundamental.index.astype(str)
    idx = pd.IndexSlice
    for stock in stocks_name:
        try:
            stock = stock.split("_")[1]
            precio_stock = precios.loc[:, EXCHANGE + "_" + stock].dropna()
            fundamental_stock = fundamental.loc[idx[:], idx[:, stock]].dropna(how="all").droplevel(axis=1, level=1)
            fundamental_stock["fecha"] = pd.to_datetime(fundamental_stock.index)
            fundamental_stock = fundamental_stock.groupby(fundamental_stock.fecha.dt.year).mean()
            fundamental_stock.index = fundamental_stock.index.astype(str)

            other_plots.plot_series(precio_stock)
            for column in fundamental_stock.columns:
                other_plots.plot_series_bar(fundamental_stock[column].dropna())
            print(dataframe.loc[dataframe.stock == stock, "description"].values[0])
            dataframe
        except Exception as e:
            logger_analyzer.error("intereseting_sectors:{}, {} ".format(stock, e))

