import requests
import os
os.chdir("../.././/")
if __name__ == "__main__":
    headers = {'Accept-Encoding': 'identity'}
    page=requests.get("https://finance.yahoo.com/video/stocks-rise-traders-mull-earnings-142550856.html",headers=headers)
    page
