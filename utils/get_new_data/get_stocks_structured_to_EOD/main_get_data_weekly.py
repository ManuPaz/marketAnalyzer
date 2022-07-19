import os
os.chdir("../../../")

from highlights import update_all_highlights
import datetime as dt


if __name__ == "__main__":
    if dt.datetime.today().weekday() in [6,1,2,3]:


        update_all_highlights()



