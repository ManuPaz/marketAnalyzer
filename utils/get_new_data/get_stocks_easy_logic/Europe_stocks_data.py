import datetime as dt
import os
os.chdir("../../../")
from datetime import timedelta
from utils.get_new_data.stocks_data_investing_implementation.get_stock_data import get_info, get_fundamental, \
    get_prices, get_dividends
from utils.get_new_data.get_stocks_easy_logic.update_stocks import upate_stocks_europe,init_indice_europe, \
    init_exchanges_countries

import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')

from utils.database import stocks_queries_string
from utils.dataframes import work_dataframes
if __name__ == "__main__":
    bd_stocks, bd_market_data, countries, exchanges = init_exchanges_countries()

    for indice in stocks_queries_string.EUROPEAN_INDICES:
        exchange = exchanges[countries[indice]]
        country = countries[indice]
        if country.lower() in stocks_queries_string.COUNTRIES_CHANGE_NAME_INVESTING_DATABASE.keys():
            country = stocks_queries_string.COUNTRIES_CHANGE_NAME_INVESTING_DATABASE[country.lower()]
        dates, isins_investing, isins, stocks, columns_bd =init_indice_europe(indice, exchange, country, bd_stocks)

        upate_stocks_europe(stocks, isins, isins_investing, country, dates, exchange, columns_bd, bd_stocks)
