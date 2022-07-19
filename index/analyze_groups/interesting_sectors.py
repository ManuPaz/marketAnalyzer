import os

import pandas as pd

from plots import other_plots
os.chdir("../../")
QUERIES = [
    "select h.*,descriptions.description  from descriptions inner join last_highlights as h on descriptions.stock=h.stock and descriptions.exchange=h.exchange where description like '%cybersecurity%' and h.MarketCapitalizationMln<10000 and h.exchange='us'",
    "select * from descriptions inner join last_highlights as h on descriptions.stock=h.stock and descriptions.exchange=h.exchange where description like '%renewable energy%' and h.MarketCapitalizationMln<10000",
    "select * from descriptions inner join last_highlights as h on descriptions.stock=h.stock and descriptions.exchange=h.exchange where description like '%quantum computing%' and h.MarketCapitalizationMln<10000"]
QUERY_I = 1
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
logger_analyzer = logging.getLogger('analyzing_data')
EXCHANGE = "US"
INDICE = None
UPDATE = False
from utils.database import bd_handler, database_functions
from utils.time_measure import time_measure
from utils.improve_performance import load_dataframe
from utils.get_new_data.get_stocks_easy_logic.update_stocks import update_stocks
if __name__ == "__main__":
    fundamental_columns = ["totalOperatingExpenses", "netIncome", "totalRevenue", "grossProfit", "totalAssets",
                           "totalCurrentLiabilities", "netDebt"]
    bd = bd_handler.bd_handler("stocks")
    query = QUERIES[QUERY_I]
    dataframe = bd.execute_query_dataframe(query)
    stocks_name = ["US_" + stock for stock in dataframe.stock.values.reshape(-1)]
    if UPDATE:
        update_stocks(EXCHANGE, INDICE, stocks_name)

    precios = time_measure.time_measure(load_dataframe.load_dataframe_bd_csv,
                                        ["data/processed/dataframes/renewable_energies.csv",
                                         database_functions.obtener_multiples_series,
                                         ["precios", "B", bd, *stocks_name]])
    precios.set_index("fecha", inplace=True)
    precios.index = pd.to_datetime(precios.index)
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
