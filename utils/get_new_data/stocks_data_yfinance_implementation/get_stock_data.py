from utils.dataframes import work_dataframes
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
from utils.database import stocks_queries_string
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
def get_fundamental(ticker,stock ):
    try:
        array=[]
        for aspect in ["quarterly_balance_sheet", "quarterly_financials", "quarterly_cashflow"]:
            array.append(getattr(ticker, aspect).transpose())
        dataframe = work_dataframes.merge(array)
        dataframe = dataframe.rename_axis(index={dataframe.index.name : "fecha"})
        if dataframe.shape[0]>0 and dataframe.shape[1]>0:
            dataframe.index = pd.to_datetime(dataframe.index)

            return dataframe
        else:
            return None

    except Exception as error:
        logger.error("US_stocks, fundamental  not found :{} {}".format(stock ,error))
        return None

def get_precios(ticker,dates,stock):
    try:
        from_date = dt.date(1990, 1, 1)
        if stock in dates.keys():
            from_date = dates[stock]
        to_date  = dt.date.today() - timedelta(days=1)
        if to_date>from_date:
            to_date = dt.datetime.strftime(to_date, "%Y-%m-%d")
            from_date = dt.datetime.strftime(from_date, "%Y-%m-%d")
            data = ticker.history(start=from_date,end=to_date)
            data["Adjusted close"]=data["Close"]
            data = data.rename_axis(index={"Date": "fecha"})
            data.index = pd.to_datetime(data.index)
            return data
        else:
            return None

    except Exception as error:
        logger.error("US_stocks, precios  not found :{} {}".format(stock, error))
        return None

def get_info(ticker,stock):
    try:
        info = ticker.calendar.transpose()

        if info.shape[0] > 1:
            info = info.iloc[-1].to_frame().transpose()
        if info.shape[0] == 0:
            info.loc[0] = np.nan
        info["fecha"] = dt.date.today()
        info.set_index("fecha", inplace=True)
        info = info.rename(columns={"Earnings Average": "eps_estimation","Revenue Average": "revenue_estimation","Earnings High":"eps_estimation_high",
                                    "Earnings Low":"eps_estimation_low","Earnings Date":"next_earnings_date",
                                    "Revenue Low":"Revenue Estimation Low","Revenue High":"Revenue Estimation High"})

        info["market_cap"]=ticker.info["marketCap"]
        info["shares_outstanding"]=ticker.info["sharesOutstanding"]

        info["adjusted_close"] = ticker.info["regularMarketPreviousClose"]
        #revenue of last quarter
        try:
            info["revenue"]=ticker.quarterly_earnings.astype(float).iloc[-1].Revenue

        except Exception as error:
            logger.error("US: {}: Failed to get revenue:{}".format(stock, error))
            info["revenue"]= np.nan

        try:
            #eps of last quater
            info["eps"] = ticker.quarterly_earnings.astype("float").iloc[-1].Earnings / float(info["market_cap"] / info["adjusted_close"])
            eps_aux = (ticker.quarterly_earnings.astype("float") / float(info["market_cap"] / info["adjusted_close"])).Earnings.sum()
            # per of last four quaters
            info["per"] = info["adjusted_close"] / eps_aux
            info["eps_annual"] = eps_aux

        except Exception as error:
            logger.error("US: {}: Failed to get eps:{}".format(stock,error))
            info["eps"]=np.nan
            info["per"] = np.nan
            info["eps_annual"]=np.nan

        try:
            # eps of last quater
            info["revenue_annual"] = (
                        ticker.quarterly_earnings.astype(float)).Revenue.sum()

        except Exception as error:
            logger.error("US: {}: Failed to get eps_annual:{}".format(stock, error))
            info["revenue_annual"] = np.nan
    except Exception as error:
        logger.error("US: {}: Failed to get info: {}".format(stock,error))
        return None
    return info

def _dividends_splits(ticker, atrr,stock):
    try:
        data = getattr(ticker,atrr)
        if data is not None:
            data= data.rename_axis(index={"Date": "fecha"})
            data.index = pd.to_datetime(data.index)
            data= data.to_frame()
            return data
    except Exception as error:
        logger.error("US: {}: Failed to get {}: {}".format(stock,atrr, error))

def get_dividends(ticker,stock):

        return _dividends_splits(ticker, "dividends",stock)
def get_splits(ticker,stock):
    return _dividends_splits(ticker, "splits",stock)
def get_info_complete_us(ticker,columns_bd,stock):
    try:
        info = {key: [value] for key, value in ticker.info.items()}
        data = pd.DataFrame.from_records(info)
        data["fecha"] = dt.date.today()
        data.set_index("fecha", inplace=True)
        drop = [e for e in data.columns if e not in columns_bd[stocks_queries_string.INFO_COMPLETE_US_WORD]]
        data.drop(drop, axis=1, inplace=True)
        return data
    except Exception as error:
        logger.error("US: {}: Failed to get info complete : {}".format(stock, error))



def get_news(ticker,stock):
    try:
        dataframe = pd.DataFrame.from_records(ticker.news)
        dataframe=dataframe.loc[:,["link","title"]]
        dataframe["fecha"]=dt.date.today()
        dataframe.set_index("link",inplace=True)
        return dataframe
    except Exception as error:
        logger.error("US: {}: Failed to get news : {}".format(stock, error))

def get_institutional_holders(ticker,stock):
    try:
        dataframe = ticker.institutional_holders
        dataframe.rename(columns={"Date Reported": "fecha","% Out":"outt","Holder":"holder"}, inplace=True)
        dataframe.set_index(["fecha","holder"],inplace=True)
        return dataframe
    except Exception as error:
            logger.error("US: {}: Failed to get institutional holders : {}".format(stock, error))
