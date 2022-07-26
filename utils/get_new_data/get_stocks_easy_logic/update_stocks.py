import difflib
import logging.config

import investpy
import yfinance as yf
import datetime as dt
from datetime import timedelta
from utils.time_measure import time_measure
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
from utils.database import database_functions, bd_handler
import numpy as np
from utils.dataframes import work_dataframes

from utils.get_new_data.stocks_data_yfinance_implementation.get_stock_data import get_fundamental, get_precios, \
    get_info, get_dividends, get_splits, get_info_complete_us, get_news, get_institutional_holders
from utils.get_new_data.stocks_data_investing_implementation.get_stock_data import \
    get_fundamental as get_fundamental_europe, get_prices as get_prices_europe, \
    get_dividends as get_dividends_europe, get_info as get_info_europe

from utils.database import stocks_queries_string
def update_data_simple(dataframe,table,bd):
    for col in dataframe.columns:
            try:
                dataframe[col] = dataframe.col.astype(float)
            except Exception:
                pass

    dataframe = dataframe[~dataframe.index.duplicated(keep='first')]
    try:
        bd.upsert_(table, dataframe)
    except Exception as e:
        logger.error("get_stock_data: Upsert of {} failed {}".format(table, e))

def update_data(columns_bd, dataframe_index, bd_stocks, name, exchange, all_numeric=True):
    if dataframe_index is not None:
        columns_dic = {e: difflib.get_close_matches(e, columns_bd, 1) for e in
                       dataframe_index.columns}
        columns_dic = {e: columns_dic[e][0] for e in columns_dic.keys() if
                       len(columns_dic[e]) > 0}
        drop = [e for e in dataframe_index.columns if e not in columns_dic.keys()]
        dataframe_index.drop(drop, axis=1, inplace=True)
        dataframe_index.rename(columns=columns_dic, inplace=True)
        dataframe_index.set_index(["stock", "exchange"], append=True, inplace=True)
        dataframe_index = dataframe_index.replace("", np.nan).replace("-", np.nan)
        if all_numeric:
            dataframe_index = dataframe_index.astype(float)
        else:
            for col in dataframe_index.columns:
                try:
                    dataframe_index[col] = dataframe_index.col.astype(float)
                except Exception:
                    pass
        if name == stocks_queries_string.FUNDAMENTAL_WORD and exchange != "US":
            dataframe_index = dataframe_index * 1000000
        dataframe_index = dataframe_index[~dataframe_index.index.duplicated(keep='first')]
        try:
            bd_stocks.upsert_(exchange + "_" + name, dataframe_index)
        except Exception as e:
            logger.error("get_stock_data: Upsert of {}:{} failed {}".format(exchange, name, e))
        dataframe_index
def init_all_stocks(exchange, indice, bd_stocks, filter=None):
    """
    Creates tables and returns columns of tables, isins of exchange and list of stocks
    :param exchange: exchange of the stocks
    :param indice: index of the stocks
    :param bd_stocks: bd to get data
    :param filter: if None inde is used, else is an array of stocks to use
    :return: dict of stock  codes as names and isins as values, list of stocks and dict of tables names as keys  and tables columns as values
    """
    for query in [stocks_queries_string.CREATE_DAILY_INFO_TABLE_BY_EXCHANGE,
                  stocks_queries_string.CREATE_PRICES_DATA_TABLE_BY_EXCHANGE,
                  stocks_queries_string.CREATE_FUNDAMENTAL_DATA_TABLE_BY_EXCHANGE,
                  stocks_queries_string.CREATE_SPLITS_TABLE_BY_EXCHANGE,
                  stocks_queries_string.CREATE_DIVIDEND_TABLE_BY_EXCHANGE]:
        bd_stocks.execute(query.format(exchange))
    columns_bd = {}
    for info_type in [stocks_queries_string.FUNDAMENTAL_WORD, stocks_queries_string.PRICES_WORD,
                      stocks_queries_string.DAILY_INFO_WORD, stocks_queries_string.SPLITS_WORD,
                      stocks_queries_string.DIVIDENDS_WORD]:
        query = "show columns from {}_{}".format(exchange, info_type)
        columns_bd[info_type] = [e[0] for e in bd_stocks.execute_query(query)]

    query = "select code,isin from stocks where exchange=%s"
    stocks_list = bd_stocks.execute_query(query, (exchange,))
    isins = {isin[0]: isin[1] for isin in stocks_list}
    if filter is None:
        stocks = database_functions.filter_by_indice(indice, exchange, bd_stocks).stock.values
    else:
        stocks = filter

    return isins, stocks, columns_bd
def init_exchanges_countries():
    """
    Gets exchange vs contry dict and index vs country dict,

    :return: dict of exchanges vs country, dict index vs coutry, bd of stocks and bd of market data
    """
    bd_stocks = bd_handler.bd_handler("stocks")
    bd_market_data = bd_handler.bd_handler("market_data")
    query_countries = "select indice,country from indice_country "
    countries = bd_market_data.execute_query(query_countries)
    countries = {e[0]: e[1] for e in countries}
    query_exchanges = "select country,code  from exchanges"
    exchanges = bd_stocks.execute_query(query_exchanges)
    exchanges = {e[0]: e[1] for e in exchanges}
    return bd_stocks, bd_market_data, countries, exchanges
def init_us_stocks(filter=None):
    """
    Gets tickers of stocks and other data suchas as dict of table name and table columns, dict of last prizes date for each stock, exchange name and bd

    :param filter:
    :return:
    """
    exchange = "US"
    indice = "S&P 500"
    bd_stocks = bd_handler.bd_handler("stocks")
    bd_stocks.execute_query(stocks_queries_string.CREATE_INFO_COMPLETE_US_TABLE)
    isins, stocks, columns_bd = init_all_stocks(
        exchange, indice, bd_stocks, filter)
    bd_stocks.execute(stocks_queries_string.CREATE_TABLE_NEWS.format(exchange))
    bd_stocks.execute(stocks_queries_string.CREATE_TABLE_INSTITUTIONAL_HOLDERS.format(exchange))
    for WORD in [stocks_queries_string.STOCKS_NEWS_WORD,
                 stocks_queries_string.INSTITUTIONAL_HOLDERS_WORD]:
        query = "show columns from {}".format(exchange + "_" + WORD)
        columns_bd[WORD] = [e[0] for e in bd_stocks.execute_query(query)]

    query = "show columns from {}".format(stocks_queries_string.INFO_COMPLETE_US_WORD)
    columns_bd[stocks_queries_string.INFO_COMPLETE_US_WORD] = [e[0] for e in bd_stocks.execute_query(query)]

    stocks = list(stocks)
    if filter is None:
        query = "select company as stock  from indices i where indice= 'nasdaq'     and not exists (select * from indices where indice='s&p 500' and company=i.company);"
        stocks = stocks + list(bd_stocks.execute_query_dataframe(query).stock.values)
    tickers = yf.Tickers(list(stocks))
    query = "select distinct stock,max(fecha) from US_precios inner join indices on indices.company=US_precios.stock group  by(stock);"
    dates = time_measure.time_measure(bd_stocks.execute_query, [query])
    dates = {date[0]: date[1] for date in dates}
    return tickers, columns_bd, dates, exchange, bd_stocks
def init_indice_europe(indice, exchange, country, bd_stocks, filter=None):
    isins, stocks, columns_bd = init_all_stocks(
        exchange, indice, bd_stocks, filter)

    isins_investing = investpy.stocks.get_stocks_dict(country=country)
    isins_investing = {e["isin"]: e for e in isins_investing}
    query = "select stock,max(fecha)  from {}_precios group by(stock);".format(exchange)
    dates = bd_stocks.execute_query(query)
    dates = {date[0]: date[1] for date in dates}

    return dates, isins_investing, isins, stocks, columns_bd
def update_stock_us(ticker, stock, columns_bd, dates, exchange, bd_stocks):
    query_date="select last(fecha) from {}_precios where stock=%s".format(exchange)
    fecha=bd_stocks.execute_query(query_date)
    if fecha is None or fecha<dt.datetime.today()-timedelta(days=1):

        dataframe_fundamental = get_fundamental(ticker, stock)

        dataframe_info = get_info(ticker, stock)
        dataframe_dividends = get_dividends(ticker, stock)
        dataframe_splits = get_splits(ticker, stock)
        dataframe_info_complete_us = get_info_complete_us(ticker, columns_bd, stock)

        dataframe_news = get_news(ticker, stock)
        dataframe_institutional_holders = get_institutional_holders(ticker, stock)
        dataframe_precios = get_precios(ticker, dates, stock)
        work_dataframes.add_constant_columns(dataframe_fundamental, dataframe_precios, dataframe_info, dataframe_splits,
                                             dataframe_dividends, dataframe_info_complete_us, dataframe_news,
                                             dataframe_institutional_holders,
                                             exchange=exchange, stock=stock)

        for WORD, dataframe in zip([stocks_queries_string.PRICES_WORD, stocks_queries_string.FUNDAMENTAL_WORD,
                                    stocks_queries_string.DIVIDENDS_WORD, stocks_queries_string.SPLITS_WORD],
                                   [dataframe_precios, dataframe_fundamental, dataframe_dividends, dataframe_splits]):
            update_data(columns_bd[WORD], dataframe, bd_stocks, WORD, exchange)

        update_data(columns_bd[stocks_queries_string.DAILY_INFO_WORD], dataframe_info, bd_stocks,
                    stocks_queries_string.DAILY_INFO_WORD, exchange, all_numeric=False)
        update_data(columns_bd[stocks_queries_string.INFO_COMPLETE_US_WORD], dataframe_info_complete_us, bd_stocks,
                    stocks_queries_string.INFO_COMPLETE_US_WORD.replace("us_", ""), exchange, all_numeric=False)
        update_data(columns_bd[stocks_queries_string.INSTITUTIONAL_HOLDERS_WORD], dataframe_institutional_holders,
                    bd_stocks,
                    stocks_queries_string.INSTITUTIONAL_HOLDERS_WORD, exchange, all_numeric=False)
        update_data(columns_bd[stocks_queries_string.STOCKS_NEWS_WORD], dataframe_news, bd_stocks,
                    stocks_queries_string.STOCKS_NEWS_WORD, exchange, all_numeric=False)
def upate_stocks_europe(stocks, isins, isins_investing, country, dates, exchange, columns_bd, bd_stocks):
    dataframe_info_index = dataframe_precios_index = dataframe_fundamental_index = dataframe_dividends_index = None
    for stock in stocks:
        try:
            if isins[stock] in isins_investing.keys():
                stock_investing = isins_investing[isins[stock]]["symbol"]
                dataframe_dividends = get_dividends_europe(stock_investing, country)
                dataframe_info = get_info_europe(stock_investing, country)
                dataframe_prizes = get_prices_europe(dates, stock_investing, stock, country)
                dataframe_fundamental = get_fundamental_europe(stock_investing, country)

                work_dataframes.add_constant_columns(dataframe_fundamental, dataframe_prizes, dataframe_info,
                                                     dataframe_dividends,
                                                     exchange=exchange, stock=stock)
                dataframe_fundamental_index = work_dataframes.concat_dataframes(dataframe_fundamental,
                                                                                dataframe_fundamental_index)
                dataframe_precios_index = work_dataframes.concat_dataframes(dataframe_prizes,
                                                                            dataframe_precios_index)
                dataframe_info_index = work_dataframes.concat_dataframes(dataframe_info, dataframe_info_index)
                dataframe_dividends_index = work_dataframes.concat_dataframes(dataframe_dividends,
                                                                              dataframe_dividends_index)

                logger.info(
                    "Europe_stocks: {}: {} - investing stock name: {} downloaded".format(country,
                                                                                         stock,
                                                                                         stock_investing))

            else:
                logger.error("Europe_stocks: Stock not found {}:{}".format(country, stock))


        except RuntimeError as e:
            logger.error("Europe_stocks: Error downloading stock {}:{}, {}".format(country, stock, e))

    update_data(columns_bd[stocks_queries_string.FUNDAMENTAL_WORD], dataframe_fundamental_index, bd_stocks,
                stocks_queries_string.FUNDAMENTAL_WORD, exchange)
    update_data(columns_bd[stocks_queries_string.PRICES_WORD], dataframe_precios_index,
                bd_stocks, stocks_queries_string.PRICES_WORD, exchange)
    update_data(columns_bd[stocks_queries_string.DAILY_INFO_WORD], dataframe_info_index, bd_stocks,
                stocks_queries_string.DAILY_INFO_WORD, exchange, all_numeric=False)
    update_data(columns_bd[stocks_queries_string.DIVIDENDS_WORD], dataframe_dividends_index, bd_stocks,
                stocks_queries_string.DIVIDENDS_WORD, exchange, all_numeric=False)
def update_stocks(exchange, indice, stocks_name):
    """
    Updates a list of stocks from an European exchange or US (index doesnt matter if   stocks_name is not None )
    :param exchange:
    :param indice:
    :param stocks_name:
    """
    if exchange == "US":
        tickers, columns_bd, dates, exchange, bd_stocks = init_us_stocks(
            filter=[stock.split("_")[1] for stock in stocks_name])
        for stock in stocks_name:
            try:
                stock = stock.split("_")[1]
                print(stock)
                update_stock_us(tickers.tickers[stock], stock, columns_bd, dates, exchange, bd_stocks)
            except KeyError as e:
                logger.error("interesting_stocks: {}_{}: {}".format(exchange, stock, e))

    else:
        bd_stocks, bd_market_data, countries, exchanges = init_exchanges_countries()
        exchange = exchanges[countries[indice]]
        country = countries[indice]
        dates, isins_investing, isins, stocks, columns_bd = init_indice_europe(indice, exchange, country, bd_stocks)
        stocks_name = [stock.split("_")[1] for stock in stocks_name]
        upate_stocks_europe(stocks_name, isins, isins_investing, country, dates, exchange, columns_bd, bd_stocks)
