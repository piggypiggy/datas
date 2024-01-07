import os
import requests
import pandas as pd
import datetime
import numpy as np
from io import StringIO, BytesIO


######### 美国能源信息署 #########

EIA_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "www.eia.gov",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}


def update_eia_weekly_us_crude_oil_production_ddata():
    se = requests.session()
    url = 'https://www.eia.gov/dnav/pet/hist_xls/WCRFPUS2w.xls'

    r = se.get(url, verify=False, headers=EIA_HEADERS)
    df = pd.read_excel(BytesIO(r.content), sheet_name='Data 1')
    print(df)


if __name__=="__main__":
    update_eia_weekly_us_crude_oil_production_ddata()



    pass

