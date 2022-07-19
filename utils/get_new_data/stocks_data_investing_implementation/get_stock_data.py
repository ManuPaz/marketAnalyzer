import numpy as np
import difflib
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
import investpy
from utils.dataframes import work_dataframes
from utils.transform_functions import  transform_units,transform_percetange
import datetime as dt
from utils.database import stocks_queries_string
import pandas as pd
from datetime import timedelta
def get_prices(dates, stock_investing,stock, country):
    from_date = dt.date(1990, 1, 1)
    if stock in dates.keys():
        from_date=dates[stock]

    to_date =dt.date.today() - timedelta(days=1)
    if to_date > from_date:
        try:
            from_date = dt.datetime.strftime(from_date, "%d/%m/%Y")
            to_date = dt.datetime.strftime(to_date, "%d/%m/%Y")
            dataframe_prizes = investpy.stocks.get_stock_historical_data(stock_investing, country,
                                                                         from_date=from_date,
                                                                         to_date=to_date, as_json=False,
                                                                         order='ascending',
                                                                         interval='Daily')

            dataframe_prizes["Adjusted_close"] = dataframe_prizes.Close
            dataframe_prizes.rename_axis("fecha", inplace=True)

            dataframe_prizes["Adjusted_close"] = dataframe_prizes.Close
            dataframe_prizes.rename_axis("fecha", inplace=True)
            return dataframe_prizes
        except Exception as error:
            logger.error("Europe_stocks, prices not found:{}:{} {}".format(country, stock, error))
            return None
    return None
def get_fundamental(stock, country):
    array = []
    for result in ["cash_flow_statement", "balance_sheet", "income_statement"]:
        try:
            array.append(
                investpy.stocks.get_stock_financial_summary(stock, country, result,
                                                            period="quarterly"))
        except Exception as e:
            logger.error("get_stock_data.get_fundamental {}:{}, {}".format(country, stock, e))
    if len(array) > 0:
        dataframe_fundamental = work_dataframes.merge(array)
        dataframe_fundamental["fecha"] = dataframe_fundamental.index
        dataframe_fundamental["fecha"] = dataframe_fundamental.fecha.map(
            lambda x: dt.datetime.strptime(x, "%b %d, %Y"))
        dataframe_fundamental.set_index("fecha", inplace=True, drop=True)
        return dataframe_fundamental
    return None


def get_info(stock, country):
    try:
        dataframe_info = investpy.stocks.get_stock_information(stock, country)
        dataframe_info["fecha"] = dt.date.today()
        dataframe_info.set_index("fecha", inplace=True, drop=True)
        dataframe_info= dataframe_info.replace("",np.nan).replace("-",np.nan)
        dataframe_info["Next Earnings Date"] = dataframe_info["Next Earnings Date"].map(
            lambda x: dt.datetime.strptime(x, "%d/%m/%Y") if isinstance(x, str) else x )
        dataframe_info.rename(columns={"P/E Ratio": "per", "EPS": "eps_annual","Revenue":"revenue_annual",
                                       "Prev. Close":"adjusted_close"}, inplace=True)
        for column in ["revenue_annual","Market Cap","Shares Outstanding"]:
            try:
                dataframe_info[column] = dataframe_info[column].astype(float)
            except Exception:
                dataframe_info[column]=dataframe_info[column].transform (lambda x:transform_units(x))

        return dataframe_info
    except Exception as error:
        logger.error("Europe_stocks, daily info not found:{}:{} {}".format(country, stock, error))
        return None


def get_dividends(stock,country):
    try:
        dataframe_dividends = investpy.stocks.get_stock_dividends(stock, country)
        dataframe_dividends["Yield"]=dataframe_dividends.Yield.transform(lambda x:transform_percetange(x))
        dataframe_dividends=dataframe_dividends.drop("Payment Date",axis=1)
        dataframe_dividends["Date"] = pd.to_datetime(dataframe_dividends.Date)
        dataframe_dividends.rename(columns={"Date": "fecha"},inplace=True )
        dataframe_dividends.set_index("fecha", inplace=True)
        return dataframe_dividends
    except Exception as e:
        logger.error("Europe_stocks, dividend not found :{}:{} {}".format(country, stock, e))
        return None
