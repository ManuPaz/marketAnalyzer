import os
import pandas as pd
def load_dataframe_bd_csv(name,function,args):
    if os.path.isfile(name):
        data=pd.read_csv(name)
    else:
        data=function(*args)
        data=data.dropna(how="all")
        data.to_csv(name)
    return data
