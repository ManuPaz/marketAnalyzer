"""
Configuration and logic dependencies:
    uses config.yaml : to events and countries
    dependencies on create_charts, create_charts_from models (charts are creted there before)
"""
import os
os.chdir("../../")
from utils.database import bd_handler

import logging.config
from config import load_config
from get_objects import get_charts, get_dashboards
config = load_config.config()
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
from utils.superset_api.functions_superset import delete_fields_model
import utils.superset_api.authenticate_superset as authenticate_superset
import json
dashboards_completed = []

add_charts = True
add_zoom = False
modifying_like_clause = False
modify_order = False
def update_chart(data):
    id = data["id"]
    data = delete_fields_model(data)
    respuesta = session.put("http://localhost:8088/api/v1/chart/{}".format(str(id)), headers=headers, json=data)
    return respuesta
def add_charts_to_macro_dashboards(chart, dashboards, nombre):
    dashboards_aux = []
    if "dashboards" in chart.keys():
        dashboards_aux = chart["dashboards"]
    for dashboard in dashboards:
        id = dashboard["id"]
        if dashboard['dashboard_title'] == nombre + " MACRO" and dashboard[
            "dashboard_title"] not in dashboards_completed:
            if id not in dashboards_aux:
                dashboards_aux.append(id)
    chart["dashboards"] = dashboards_aux

    update_chart(chart)
def modify_order(chart):
    params = chart["params"]
    params = json.loads(params)
    params["order_by_cols"] = ["[\"Date\", false]"]
    chart["params"] = json.dumps(params)

    if "id" in chart.keys():
        update_chart(chart)
def set_zoom_other_equities(section, session):
    params = json.loads(section["params"])
    params["zoomable"] = True
    section["params"] = json.dumps(params)
    update_chart(section)
def modify_like_clause(section, name):
    print(name)
    id = section["id"]
    params = json.loads(section["params"])
    for filter in params["adhoc_filters"]:
        if filter["subject"] == "event":
            event = filter["comparator"].split(" ")[0]
            if name in config["superset"]["filters_replace"].keys() and event in config["superset"]["filters_replace"][
                name].keys():
                filter["comparator"] = filter["comparator"].replace(event,
                                                                    config["superset"]["filters_replace"][name][event])

                section["params"] = json.dumps(params)
                update_chart(section)
if __name__ == "__main__":
    bd = bd_handler.bd_handler("stocks")
    headers, session = authenticate_superset.authenticate()
    slices_names = []
    charts = get_charts(session, headers)
    dashboards = get_dashboards(session, headers)

    for section in charts:
        slices_names.append(section["slice_name"])

    for section in charts:
        nombre_database = section["datasource_name_text"]
        params = json.loads(section["params"])
        if add_zoom and nombre_database in config["superset"]["list_other_equities"]:
            set_zoom_other_equities(section, session)
        if modifying_like_clause:
            name = None
            if nombre_database == "market_data.calendar" and section["slice_name"].split(" ")[0] == "UNITED":
                name = "UNITED" + " " + section["slice_name"].split(" ")[1]
            elif nombre_database == "market_data.calendar":
                name = section["slice_name"].split(" ")[0]
            if name is not None:
                modify_like_clause(section.copy(), name)
        if add_charts:
            nombre = section["slice_name"]
            esta = False
            aux = nombre.split(" ")[0].upper()
            if aux in config["superset"]["lista_paises"]:
                esta = True
                nombre = aux
            if len(nombre.split(" ")) > 1:
                aux = nombre.split(" ")[0].upper() + " " + nombre.split(" ")[1].upper()
                if aux in config["superset"]["lista_paises"]:
                    esta = True
                    nombre = aux

            if nombre_database in ["market_data.calendar", "market_data.calendar_processed"] and esta:
                add_charts_to_macro_dashboards(section.copy(), dashboards, nombre)

        if modify_order and nombre_database == "market_data.calendar" and section["slice_name"].split(" ")[
            -1] == "TABLE":
            modify_order(section)
