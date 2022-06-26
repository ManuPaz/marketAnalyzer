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
    model["slice_name"] = country.upper() + " " + KEY_STRING + " " + event.replace("%", " ").upper() + " " + params[
        "viz_type"].upper()
    model["params"] = json.dumps(params)
def add_chart(model, slices_names):
    if model["slice_name"] not in slices_names:
        session.post("http://localhost:8088/api/v1/chart/", headers=headers, json=model)
        slices_names.append(model["slice_name"])
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
    for section in charts:
        slices_names.append(section["slice_name"])
        dir = "assets/superset/create_chart_schemas/"
    models = []
    for name_file in os.listdir(dir):
        with open(dir + name_file, "r") as file:
            models.append(json.load(file))
    periods = [macro_queries_string.YOY_STRING, macro_queries_string.MOM_STRING, macro_queries_string.QOQ_STRING]
    countries = bd.execute_query__get_list(
        macro_queries_string.GET_COUNTRIES_FROM_CALENDAR.format(macro_queries_string.TABLE_CALENDAR_PROCESSED))
    for country in countries:
        events = bd.execute_query__get_list(
            macro_queries_string.GET_MACRO_EVENT_BY_COUNTRY.format(macro_queries_string.TABLE_CALENDAR_PROCESSED),
            (country,))
        for event in events:
            for model_aux in models:
                model = model_aux.copy()
                if model["slice_name"] not in slices_names:
                    if model[
                        "datasource_name"] == macro_queries_string.DATA_BASE + "." + macro_queries_string.TABLE_CALENDAR_PROCESSED:
                        set_params(model, country, event, KEY_STRING)
                        add_chart(model, slices_names)

                    elif model[
                        "datasource_name"] == macro_queries_string.DATA_BASE + "." + macro_queries_string.TABLE_CALENDAR:
                        add_one = False
                        for period in periods:
                            model_period = model_aux.copy()
                            data = bd.execute_query_dataframe(macro_queries_string.GET_DATA_BY_EVENT_AND_ZONE.format(
                                macro_queries_string.TABLE_CALENDAR), (country,
                                                                       event + "%" + period + "%"))
                            if check_data(data):
                                add_one = True
                                params = json.loads(model["params"])
                                set_params(model, country, event + "%" + period + "%", KEY_STRING)
                                add_chart(model, slices_names)
                        if not add_one:
                            data = bd.execute_query_dataframe(macro_queries_string.GET_DATA_BY_EVENT_AND_ZONE.format(
                                macro_queries_string.TABLE_CALENDAR), (country,
                                                                       event + "%"))
                            if check_data(data):
                                add_one = True
                                params = json.loads(model["params"])
                                set_params(model, country, event + "%", KEY_STRING)
                                add_chart(model, slices_names)
