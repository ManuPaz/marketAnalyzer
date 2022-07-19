import numpy as np
import datetime as dt
def transform_units(x):
    if (x.find("M")!=-1):
        x=float(x.replace("M",""))*1000000
    return x

def transform_percetange(x):
    if (isinstance(x,str)):
        try:
            x=float(x.replace("%",""))*0.01
            return  x
        except Exception:
            return np.nan
    return  np.nan


def time_array_sto_str(array,fmt):
    return [dt.datetime.strptime(e,fmt ) for e in array]
