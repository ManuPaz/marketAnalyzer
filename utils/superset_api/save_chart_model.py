"""
Configuration and logic dependencies:
    uses utils/database/macro_queries_string
    charts to use as models must be created using SUPERSET GUI
"""
import os
os.chdir("../../")
from utils.database import bd_handler,macro_queries_string
import logging.config
from get_objects import get_charts
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
import json
import utils.superset_api.authenticate_superset as authenticate_superset
file_to_save = "schema_example.json"
chart_model_to_save = "cpi_pmi"
from functions_superset import format_chart_json_to_save
if __name__ == "__main__":
    bd = bd_handler.bd_handler("stocks")
    headers, session = authenticate_superset.authenticate()
    get_charts(session, headers)
    slices_names = []

    charts = get_charts(session, headers)
    print("Number of charts {}".format(len(charts)))
    for section in charts:
        array_tables=[macro_queries_string.DATA_BASE+"."+macro_queries_string.TABLE_CALENDAR,
                      macro_queries_string.DATA_BASE+"."+macro_queries_string.TABLE_CALENDAR_PROCESSED]
        if (section["datasource_name_text"] in array_tables):
            if section["slice_name"] == chart_model_to_save:
                data = json.dumps(section.copy())
                data = data.replace("\n", "")
                data = json.loads(data)
                data = format_chart_json_to_save(data)
                with open("assets/superset/create_chart_schemas/" + file_to_save, "w") as file:
                    json.dump(data, file)

