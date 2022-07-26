import os
os.chdir("../")
print(os.getcwd())
EXCHANGE = "MC"
STOCK = "ACS"
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
logger_analyzer = logging.getLogger('analyzing_data')
from utils.database import bd_handler
import difflib
import datetime as dt
if __name__ == '__main__':
    bd = bd_handler.bd_handler("stocks")
    query = "show columns from {}".format(EXCHANGE + "_fundamental")
    columns = bd.execute_query(query)
    columns = [e[0] for e in columns]
    print(columns)
    fechaAnt = None
    while 1:
        fecha = input("Date:\n")
        if fecha == "" and fechaAnt is not None:
            fecha = fechaAnt
        column = input("Column:\n")
        value = input("Value:\n")

        try:
            fecha_date_format = dt.datetime.strptime(fecha, "%Y-%m-%d")
            column_insert = difflib.get_close_matches(column, columns, 1)
            if len(column_insert) > 0:
                column_insert = column_insert[0]
                print("Columna a insertar: {}".format(column_insert))
                try:
                    value = float(value)
                    print("Value a insertar {}".format(value))
                    seguir = input()
                    if seguir == "":
                        update_query = "update {}_fundamental set {}=%s where fecha=%s and stock=%s".format(EXCHANGE,
                                                                                                            column_insert)
                        bd.execute(update_query, (value, fecha, STOCK))
                    else:
                        logger.info("NO se inserta")
                except Exception as e:
                    logger.error("Value erroneo")
            else:
                logger.error("Columna  erronea")



        except Exception as e:
            logger.error("Fecha erronea")
        continuar = input("Seguir?")
        if continuar != "":
            break
        fechaAnt = fecha
