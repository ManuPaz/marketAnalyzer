import os
os.chdir("../../")
from utils.time_measure import time_measure
from utils.improve_performance import load_dataframe
from utils.database import stocks_queries_string
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', 500)
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
from utils.database import bd_handler, database_functions
dir_name="reports/stocks/"
def analyze_recession(recession,data,**args):

    data_recession=data.loc[recession[0]:recession[1]]
    data_recession
    recession_period_string="from "+dt.datetime.strftime(recession[0].date(),"%Y-%m-%d")+"; to "+ dt.datetime.strftime(recession[1].date(),"%Y-%m-%d")
    columns_drop = [i for i in data_recession.isna().sum().index if
                    data_recession.isna().sum()[i] > data_recession.shape[0] / 10]
    logger.info("recesion_analisis: {}, stocks to drop {}, stocks number (plus index) {}".format(recession,columns_drop,
                                                                        data_recession.shape[1]-len(columns_drop)))

    data_recession=data_recession.loc[:,set(data_recession.columns)-set(columns_drop)]
    changes=(data_recession.iloc[-1]-data_recession.iloc[0])/data_recession.iloc[0]
    changes=changes.to_frame("change")
    changes["action"] = changes.index
    for key,value in args.items():
        changes[key] = changes["action"].map(lambda x: value[x] if x!="index" else None)
    changes.drop("action",inplace=True,axis=1)
    changes.sort_values(by="change").to_csv(dir_name+event+"_"+str(recession_period_string)+".csv")

    return changes,recession_period_string

if __name__ == "__main__":
    country = "italy"
    exchange = "MI"
    event = "FTSE MIB"
    tipo = "index"
    freq = "B"
    col_name = "close"
    dir_name=dir_name+event+"/recession/"
    if not os.path.isdir(dir_name):
        os.makedirs(dir_name)
    periods = [["2000-03-01", "2002-09-01"], ["2007-12-01", "2009-03-01"],
               ["2010-01-01", "2012-07-01"], ["2010-01-01", "2012-07-01"],
               ["2015-05-01", "2016-06-01"], ["2020-02-01", "2020-10-01"],
               ["2022-01-01","2022-07-01"]]
    periods=[(dt.datetime.strptime(e[0], "%Y-%m-%d"), dt.datetime.strptime(e[1], "%Y-%m-%d")) for e in periods]
    bd_market_data = bd_handler.bd_handler("market_data")
    bd_stocks = bd_handler.bd_handler("stocks")
    index = database_functions.get_series_activos_diferentes_de_acciones(country, event, tipo, freq, col_name,
                                                                         bd_market_data)



    stocks = database_functions.filter_by_indice(event, exchange, bd_stocks)
    stocks
    data = time_measure.time_measure(load_dataframe.load_dataframe_bd_csv,
                                     ["data/processed/dataframes/{}_recession.csv".format(event),
                                      database_functions.obtener_multiples_series, ["precios", "B", bd_stocks,
                                                                                    *stocks.accion.values.reshape(-1)]])

    data.set_index("fecha",inplace=True)
    data.index=pd.to_datetime(data.index)
    data["index"]=index

    names={}
    sectors={}
    descriptions={}
    no_numerica_info=[names,sectors,descriptions]
    queries=[stocks_queries_string.STOCK_NAMES_BY_EXCHANGE,stocks_queries_string.DESCRIPTIONS_BY_EXCHANGE,
             stocks_queries_string.SECTORS_BY_EXCHANGE]
    for i,query in enumerate(queries):

        values= bd_stocks.execute_query(query,(exchange,))
        no_numerica_info[i].update({e[0]: e[1] for e in values})
    changes_total=pd.DataFrame()
    changes_total.index=list(stocks.accion)
    for recession in periods:
        changes,recession_period_string=analyze_recession(recession, data,**{"names":names,"sectors":sectors,"descriptions":descriptions})
        if recession!=periods[-1]:
            changes_total[recession_period_string]=changes.change

    changes_total.mean(axis=1).to_csv(dir_name+event+"_"+"mean.csv")
