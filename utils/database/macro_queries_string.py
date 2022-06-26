YOY_STRING = "YoY"
MOM_STRING = "MoM"
QOQ_STRING = "QoQ"
TABLE_CALENDAR= "calendar"
TABLE_CALENDAR_PROCESSED= "calendar_processed"
DATA_BASE="market_data"
MONTHS_NAMES_CALENDAR=["Jan", "Feb", "Mar",
        "Apr", "Apr", "May",
        "Jun", "Jul", "Aug",
        "Sep", "Oct", "Nov",
        "Dec"]
QUARTERS_NAMES_CALENDAR=["Q1","Q2","Q3","Q4"]
CREATE_TABLE_MACRO_PROCESSED="create table if not exists {} " \
                           "(Date date, " \
                           "zone varchar(100)," \
                           "event varchar (100)," \
                           "actual double," \
                           "importance varchar(100)," \
                           "currency varchar(100)," \
                            "PRIMARY KEY(date,zone,event))"
GET_MACRO_EVENT_BY_COUNTRY="select distinct(event) from {} where zone=%s"
GET_COUNTRIES_FROM_CALENDAR="select distinct(zone) from {}"
GET_DATA_BY_EVENT_AND_ZONE="select * from {} where zone=%s and event like %s"
