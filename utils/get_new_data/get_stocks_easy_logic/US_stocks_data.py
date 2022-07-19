import os


os.chdir("../../../")
from utils.get_new_data.get_stocks_easy_logic.update_stocks import update_stock_us,init_us_stocks
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')


if __name__ == "__main__":
    tickers, columns_bd, dates,exchange,bd_stocks =init_us_stocks()
    for stock, ticker in tickers.tickers.items():
        print(stock)
        update_stock_us(ticker, stock, columns_bd, dates, exchange, bd_stocks)

