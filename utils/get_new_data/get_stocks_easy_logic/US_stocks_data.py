import os


os.chdir("../../../")
from utils.get_new_data.get_stocks_easy_logic.update_stocks import update_stock_us,init_us_stocks
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
import datetime as dt
from datetime import  timedelta

if __name__ == "__main__":
    tickers, columns_bd, dates,exchange,bd_stocks =init_us_stocks()
    for stock, ticker in tickers.tickers.items():
        query_date = "select max(fecha) from {}_precios where stock=%s".format(exchange)
        fecha = bd_stocks.execute_query(query_date, (stock,))
        print(stock)
        if fecha is None or fecha[0][0] < dt.date.today() - timedelta(days=1):

            ticker=tickers.tickers[stock]
            update_stock_us(ticker, stock, columns_bd, dates, exchange, bd_stocks)

