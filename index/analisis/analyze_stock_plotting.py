import os
os.chdir("../../")
from utils.database import database_functions,bd_handler
from plots.plot_specific_series import plot_specific_series
if __name__=="__main__":
    bd=bd_handler.bd_handler("stocks")
    fecha_corte="2010-01-01"
    stock="US_S"
    nombre_serie = ["netIncome","totalrevenue","totalAssets","totalLiab"]
    freq="D"
    plot_specific_series(nombre_serie, stock, freq, fecha_corte, bd)

