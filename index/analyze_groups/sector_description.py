import os
os.chdir("../../")
from config import load_config
config = load_config.config()
import logging.config
import pandas as pd
from plots import other_plots
from utils.time_measure import time_measure
from utils.improve_performance import   load_dataframe
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
from utils.database import bd_handler, database_functions
energies=[ "nuclear power", "solar power","wind power","nuclear energy","renewable energy"]
update=True
def find_match(x):
    for energy in  energies:
        if x.find(energy)!=-1:
            return  energy

def pct_changes(x,dias):
    pass

if __name__ == "__main__":
    bd = bd_handler.bd_handler("stocks")
    stocks_admiral = database_functions.filter_by_broker("admiralmarkets", bd=bd).accion.values.reshape(-1)
    stocks_degiro = database_functions.filter_by_broker("admiralmarkets", bd=bd).accion.values.reshape(-1)
    stocks = database_functions.filter_by_description(*energies, bd=bd)
    stocks_name = stocks.accion.values.reshape(
        -1)
    stocks["type"]=stocks["description"].transform(lambda x:find_match(x))
    stocks_list = list(set(stocks_admiral).intersection(set(stocks_name)))
    logger.info("sector_description: number of stocks to work {}".format(len(stocks_name)))
    data = time_measure.time_measure(load_dataframe.load_dataframe_bd_csv,["data/processed/dataframes/sector_energies.csv",database_functions.obtener_multiples_series,["precios", "B", bd, *stocks_name]])
    if data.index.name != "fecha":
        data = data.set_index("fecha")
        data.index=pd.to_datetime(data.index)
    query="select code as exchange, country from exchanges"
    countries=bd.execute_query(query)
    countries = {e[0]: e[1] for e in countries}
    stocks_real = stocks.loc[stocks.accion.isin(data.columns)]
    stocks_real["country"] = stocks_real.exchange.transform(lambda x: countries[x])
    stocks_name_real = list(set(stocks_name).intersection(set(data.columns)))
    for column in ["exchange","country","type"]:
        stocks_grouped = stocks_real.groupby(column, as_index=False).count()
        stocks_grouped.rename(columns={"accion": "count"}, inplace=True)
        other_plots.bar_plot(stocks_grouped, column, "count", rotation=60)





