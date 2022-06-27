

def delete_fields_model(data):
    for e in ['changed_by', 'changed_by_name', 'changed_by_url', 'changed_on_delta_humanized',
              'changed_on_utc', 'created_by', 'edit_url', 'id', 'last_saved_at', 'last_saved_by',
              'url', 'thumbnail_url', "table", "datasource_url", "datasource_name_text", "description_markeddown",
              "owners"]:
        if e in data.keys():
            data.pop(e)
    return data
def create_chart(data,headers,session):
    format_chart_json_to_save(data)
    session.post("http://localhost:8088/api/v1/chart/", headers=headers, json=data)
def format_chart_json_to_save(data):
    if "datasource_name_text" in data.keys():
        data["datasource_name"] = data["datasource_name_text"]
    data = delete_fields_model(data)
    if data['datasource_name'] == "market_data.calendar":
        data["owners"] = [1]
    return data
