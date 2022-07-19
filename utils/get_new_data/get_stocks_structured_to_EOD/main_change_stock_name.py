
import os
os.chdir("../../../")
from utils.database import bd_handler
def change_name(old_stock_name,new_stock_name,exchange):

    bd=bd_handler.bd_handler("stocks")
    query_tables="show tables"
    tablas=[e[0] for e in bd.execute_query(query_tables)]
    for tabla in tablas:
        columns=[e[0] for e in bd.execute_query("show columns from {}".format(tabla))]

        for stock_column_name in ["stock","code","company"]:
            for group_column_name in ["exchange", "indice"]:
                if  stock_column_name  in columns and group_column_name in columns :
                    print(tabla)
                    if group_column_name=="exchange":
                        execute="update {} set {}=%s where {}=%s and {}=%s"
                        bd.execute(execute.format(tabla,stock_column_name,stock_column_name,group_column_name),(new_stock_name,old_stock_name,exchange))
                        break
                    else:
                        execute = "update {} set {}=%s where {}=%s "
                        bd.execute(execute.format(tabla, stock_column_name, stock_column_name),
                                   (new_stock_name, old_stock_name,))
                        break


if __name__ == "__main__":
    old_stock_name = "FB"
    new_stock_name = "META"
    exchange = "US"
    change_name(old_stock_name,new_stock_name,exchange)

