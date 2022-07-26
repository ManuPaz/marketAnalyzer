

import os
os.chdir("../../../")
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from utils.get_new_data.get_stocks_easy_logic.update_stocks import update_data_simple
from utils.database import  bd_handler
EXCHANGE="US"
if __name__ == "__main__":
    import requests
    bd=bd_handler.bd_handler("stocks")
    URL = "https://www.marketwatch.com/tools/earnings-calendar"
    page = requests.get(URL)
    page
    soup = BeautifulSoup(page.text, "html.parser")
    results = soup.find_all("div",class_="element element--table table--fixed")
    results=results+soup.find_all("div",class_="element element--table table--fixed is-selected")
    report_dates={}
    for result in results:
        date = result.attrs["data-tab-pane"]
        report_dates[date] = {}
        stocks = result.find_all("tr", class_="table__row")
        for stock in stocks[1:]:
            stock_name = stock.find_all("a", class_="link")[2].text
            report_dates[date][stock_name] = {}
            cells = stock.find_all("div", class_="cell__content")
            report_dates[date][stock_name]["date"] = cells[3].text
            report_dates[date][stock_name]["estimate"] = cells[4].text
            report_dates[date][stock_name]["actual"] = cells[5].text
    reform = {(level1_key, level2_key): list(values.values())
              for level1_key, level2_dict in report_dates.items()
              for level2_key, values in level2_dict.items()}
    dataframe = pd.DataFrame.from_dict(reform).transpose()
    dataframe.columns = ["date", "estimate", "actual"]
    dataframe.index.names = ["report_date", "stock"]
    dataframe.index = dataframe.index.set_levels(pd.to_datetime(dataframe.index.levels[0]), 0)
    dataframe["date"] = pd.to_datetime(dataframe.date)

    for column in dataframe.columns:
        try:
            dataframe[column]=dataframe[column].str.replace(",","")
        except Exception as e:
            pass
    dataframe = dataframe.replace("N/A", np.nan)
    dataframe["exchange"]=EXCHANGE
    update_data_simple(dataframe,"calendarioResultadosNew",bd)


