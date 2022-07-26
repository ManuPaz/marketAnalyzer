import os

import pandas as pd
from pandas_ods_reader import read_ods
os.chdir("../../")
index = "ibex 35"
stock = "acs"
dir = "data/processed/dataframes/"
from utils.improve_performance.cython.work_dataframes import diff_to_absolute
if __name__ == '__main__':
    data = read_ods(dir + index + "/" + stock + "/results.ods", 1)
    data = data.dropna(how="all")
    data.columns = data.iloc[0, :]
    data = data.iloc[1:, ]
    data["year"] = data.year.astype(int)
    data.set_index("year", inplace=True, drop=True)
    data = data.astype(float)
    data.sort_index(inplace=True)
    for column in data.columns:
        if column.find("gdp") != -1 or column.find("cpi") != -1:
            data.loc[:, column] = diff_to_absolute(data[column].values, 1)
    q_data = read_ods(dir + index + "/" + stock + "/results_quarterly.ods", 1)
    q_data = q_data.dropna(how="all")
    q_data.columns = q_data.iloc[0, :]
    q_data = q_data.iloc[1:, ]
    q_data["date"] = pd.to_datetime(q_data["date"])
    q_data.set_index("date", inplace=True, drop=True)
    q_data = q_data.astype(float)
    q_data.sort_index(inplace=True)
    for column in q_data.columns:
        if column.find("gdp") != -1 or column.find("cpi") != -1:
            q_data.loc[:, column] = diff_to_absolute(q_data[column].values, 1)
    cor = data.corr()
    q_cor = q_data.corr()
