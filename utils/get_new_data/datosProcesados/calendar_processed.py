#uses config.yaml : to events and countries
#uses utils/database/macro_queries_string
import os
os.chdir("../../../")
import numpy as np
from utils.database import macro_queries_string as bd_macro_string, bd_handler
from config import load_config
from utils.dataframes.work_dataframes import diff_to_absolute
config = load_config.config()
from dateutil import relativedelta
import datetime as dt
import pandas as pd
events_to_save=config["macro"]["events_to_save"]
key_words = ["core"]
periods = {bd_macro_string.MOM_STRING: 1, bd_macro_string.QOQ_STRING: 3, bd_macro_string.YOY_STRING: 12}
if __name__ == "__main__":
    bd = bd_handler.bd_handler("market_data")

    query = bd_macro_string.CREATE_TABLE_MACRO_PROCESSED.format(bd_macro_string.TABLE_CALENDAR_PROCESSED)
    bd.execute(query)
    countries = [e[0] for e in (bd.execute_query(bd_macro_string.GET_COUNTRIES_FROM_CALENDAR.format(bd_macro_string.TABLE_CALENDAR)))]
    events = []
    for country in countries:
        if country in config["macro"]["countries_working"]:
            events_aux = [e[0] for e in (bd.execute_query(bd_macro_string.GET_MACRO_EVENT_BY_COUNTRY.format(bd_macro_string.TABLE_CALENDAR), (country,)))]
            for event in events_aux:

                for month in bd_macro_string.MONTHS_NAMES_CALENDAR:
                    if event.find(month) != -1:
                        events.append(event.replace("  (" + month + ")", ""))
                        break
                for qoq in bd_macro_string.QUARTERS_NAMES_CALENDAR:
                    if event.find(qoq) != -1:
                        events.append(event.replace("  (" + qoq + ")", ""))
                        break
            events=list(np.unique(events))
            events.sort()
            while len(events) > 0:
                event = events.pop(0)
                save_event = False

                for cadena in [bd_macro_string.MOM_STRING, bd_macro_string.YOY_STRING,
                               bd_macro_string.QOQ_STRING]:
                    event = event.replace(" (" + cadena + ")", "").replace(cadena, "").strip()
                for e in events_to_save:
                    if event.upper().find(e.upper()) != -1:
                        save_event = True
                        break

                if save_event:
                    data = bd.execute_query_dataframe(bd_macro_string.GET_DATA_BY_EVENT_AND_ZONE_EQUALS.format(bd_macro_string.TABLE_CALENDAR_PROCESSED),
                                                      (country, event))
                    add=data is None
                    if data is not None:
                        data["Date"] = pd.to_datetime(data["Date"])
                        if len(data) > 0 and data.loc[
                            data.index[-1], "Date"].to_pydatetime() < dt.datetime.now() - relativedelta.relativedelta(
                                months=1):
                            add=True
                    if add:
                        dic_dataframes = {}
                        for cadena in [bd_macro_string.MOM_STRING, bd_macro_string.YOY_STRING,
                                       bd_macro_string.QOQ_STRING]:

                            data = bd.execute_query_dataframe(
                                bd_macro_string.GET_DATA_BY_EVENT_AND_ZONE_LIKE.format(bd_macro_string.TABLE_CALENDAR),
                                (country, event  +" ("+ cadena+")" + "%"))

                            if data is None:
                                lenght = 0

                            else:
                                lenght = len(data)
                                data["Date"] = pd.to_datetime(data.Date)
                            if lenght > 0:
                                dic_dataframes[cadena] = [data, lenght]
                        if      len(dic_dataframes)>0:
                            lenght = max([e[1] for e in dic_dataframes.values()])
                        if lenght == 0:
                            data = bd.execute_query_dataframe(
                                bd_macro_string.GET_DATA_BY_EVENT_AND_ZONE_DOUBLE_LIKE.format(bd_macro_string.TABLE_CALENDAR),
                                (country, event + "  (%",event))
                            if data is not None:
                                lenght = len(data)
                                if lenght > 0:
                                    dic_dataframes[""] = [data, lenght]
                                    data["Date"] = pd.to_datetime(data.Date)

                        if      len(dic_dataframes)>0:
                            lenght = max([e[1] for e in dic_dataframes.values()])

                            if lenght > 30:
                                cadenas_keys = list(dic_dataframes.keys())
                                cadenas_keys.sort(key=lambda x: int(dic_dataframes[x][0].Date.iloc[-1].to_pydatetime()>= dt.datetime.now() - relativedelta.relativedelta(
                                        months=4))*dic_dataframes[x][1], reverse=True)
                                cadena=cadenas_keys[0]
                                data = dic_dataframes[cadenas_keys[0]][0]
                                if data.loc[data.index[-1], "Date"] >= dt.datetime.now() - relativedelta.relativedelta(
                                        months=4):
                                    if cadena != "":
                                        data = diff_to_absolute(data, "actual", periods[cadena])
                                        data["event"] = data["event"].transform(
                                            lambda x: str(x).replace(" (" + cadena + ")", "").strip())
                                    for month in bd_macro_string.MONTHS_NAMES_CALENDAR:
                                        data["event"] = data["event"].transform(
                                            lambda x: str(x).replace("  (" + month+ ")", "").strip())
                                    for qoq in bd_macro_string.QUARTERS_NAMES_CALENDAR:
                                        data["event"] = data["event"].transform(
                                            lambda x: str(x).replace("  (" + qoq + ")", "").strip())
                                    data=data.set_index("Date")
                                    data=data.drop(["forecast","id"],axis=1)
                                    data=data[~data.index.duplicated(keep='last')]
                                    print(data.tail())

                                    bd.upsert_(bd_macro_string.TABLE_CALENDAR_PROCESSED, data)
