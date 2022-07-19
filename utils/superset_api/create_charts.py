"""
Configuration and logic dependencies:
    uses config.yaml : to events and countries
    uses utils/database/macro_queries_string
    dependencies on  save_chart_models to save the  SUPERSET models
"""
import os
os.chdir("../../")
from utils.database import bd_handler
import logging.config
from config import load_config
import pandas as pd
import datetime as dt
from dateutil import relativedelta
from utils.database import macro_queries_string
from get_objects import get_charts
config = load_config.config()
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
import json
import utils.superset_api.authenticate_superset as authenticate_superset
from config import load_config
config = load_config.config()
def set_params(model, country, event, KEY_STRING):
    subjects = {"zone": country, "event": event}
    params = json.loads(model["params"])
    for filter in params["adhoc_filters"]:
        if filter["subject"] in subjects.keys():
            filter["comparator"] = subjects[filter["subject"]]
    model["owners"] = [1]
    model["slice_name"] = country.upper() + " " + KEY_STRING + " " + event.replace("%", " ").replace("(","").replace(")","").strip().upper() + " " + params[
        "viz_type"].upper()
    model["params"] = json.dumps(params)
def add_chart(model, slices_names):
    if model["slice_name"] not in slices_names:
        respuesta=session.post("http://localhost:8088/api/v1/chart/", headers=headers, json=model)
        slices_names.append(model["slice_name"])
        return respuesta
def check_data(data):
    if data is not None:
        data["Date"] = pd.to_datetime(data["Date"])
        if len(data) > 0 and data.loc[
            data.index[-1], "Date"].to_pydatetime() > dt.datetime.now() - relativedelta.relativedelta(months=4):
            return True
    return False
if __name__ == "__main__":
    KEY_STRING = "MACRO"
    bd = bd_handler.bd_handler("market_data")
    headers, session = authenticate_superset.authenticate()
    get_charts(session, headers)
    slices_names = []

    charts = get_charts(session, headers)
    print("Number of charts {}".format(len(charts)))
    dic_ids={}
    for section in charts:
        slices_names.append(section["slice_name"])
        dic_ids[section["slice_name"]]=section["id"]
        dir = "assets/superset/create_chart_schemas/"
    models = []
    names_models=[]
    for name_file in os.listdir(dir):
        if name_file.find("two_conditions")==-1:
            with open(dir + name_file, "r") as file:
                models.append(json.load(file))
                names_models.append(name_file)
    periods = [macro_queries_string.YOY_STRING, macro_queries_string.MOM_STRING, macro_queries_string.QOQ_STRING]
    countries = bd.execute_query__get_list(
        macro_queries_string.GET_COUNTRIES_FROM_CALENDAR.format(macro_queries_string.TABLE_CALENDAR_PROCESSED))
    for country in countries:
        events = bd.execute_query__get_list(
            macro_queries_string.GET_MACRO_EVENT_BY_COUNTRY.format(macro_queries_string.TABLE_CALENDAR_PROCESSED),
            (country,))
        for event in events:
            print(country,event)
            for k,model_aux in enumerate(models):
                    model = model_aux.copy()
                #if model["slice_name"] not in slices_names:
                    if model[
                        "datasource_name"] == macro_queries_string.DATA_BASE + "." + macro_queries_string.TABLE_CALENDAR_PROCESSED:
                        set_params(model, country, event, KEY_STRING)
                        add_chart(model, slices_names)

                    elif model[
                        "datasource_name"] == macro_queries_string.DATA_BASE + "." + macro_queries_string.TABLE_CALENDAR:
                        add_one = False
                        for period in periods:
                            model_period = model_aux.copy()
                            data = bd.execute_query_dataframe(macro_queries_string.GET_DATA_BY_EVENT_AND_ZONE_LIKE.format(
                                macro_queries_string.TABLE_CALENDAR), (country,
                                                                       event  +" ("+  period+")"  + "%"))
                            if check_data(data):
                                add_one = True
                                params = json.loads(model["params"])
                                set_params(model, country, event +" ("+  period+")"  +"%", KEY_STRING)
                                add_chart(model, slices_names)
                        if not add_one:
                            data1 = bd.execute_query_dataframe(
                                macro_queries_string.GET_DATA_BY_EVENT_AND_ZONE_LIKE.format(
                                    macro_queries_string.TABLE_CALENDAR), (country,
                                                                           event + "  (%"))
                            data = bd.execute_query_dataframe(macro_queries_string.GET_DATA_BY_EVENT_AND_ZONE_DOUBLE_LIKE.format(
                                macro_queries_string.TABLE_CALENDAR), (country,
                                                                       event + "  (%",event))
                            if len(data)>0:
                                add_one = True
                                params = json.loads(model["params"])
                                if data1 is None or len(data1)==0:
                                    set_params(model, country,   event, KEY_STRING)
                                    add_chart(model, slices_names)
                                else:
                                    with open(dir + names_models[k].replace(".json","_two_conditions.json"), "r") as file:
                                        m_aux=(json.load(file))
                                        name_chart= country.upper() + " " + KEY_STRING + " " + event.replace(
                                            "%", " ").replace("(", "").replace(")", "").strip().upper() + " " + params[
                                                                  "viz_type"].upper()

                                        subjects = {"zone": country, "event": event}
                                        params = json.loads(m_aux["params"])
                                        for filter in params["adhoc_filters"]:
                                            if filter["subject"] in subjects.keys():
                                                filter["comparator"] = subjects[filter["subject"]]
                                            elif filter["subject"] is None:
                                                filter["sqlExpression"]=filter[ "sqlExpression"].\
                                                    replace(filter["sqlExpression"].split(" ")[2],"'"+event+"'").\
                                                    replace(filter["sqlExpression"].split(" ")[6],"'"+event + "  (%"+"'")

                                        m_aux["owners"] = [1]
                                        m_aux["slice_name"] =name_chart
                                        m_aux["params"] = json.dumps(params)
                                        respuesta=add_chart(m_aux, slices_names)
                                        respuesta


