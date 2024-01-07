import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
from io import StringIO, BytesIO


##### 日本财务省 #####

# japanese government bond interest rate
def update_jgb_rate():
    se = requests.session()

    JGB_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                           'Host': 'www.mof.go.jp'}

    HISTORY_DATA_URL = 'https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv'
    CURRENT_DATA_URL = 'https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv'

    path = os.path.join(interest_rate_dir, 'jgb'+'.csv')
    print('japanese government bond interest rate')
    if not(os.path.exists(path)):
        while (1):
            try:
                r = se.get(HISTORY_DATA_URL, headers=JGB_HEADERS)
                df = pd.read_csv(StringIO(r.text), header=1)
                break
            except Exception as e:
                print(e)
                time.sleep(10)

        df.replace("-", np.nan, inplace=True)
        df.rename(columns={'Date':'time'}, inplace=True)
        df['time'] = pd.to_datetime(df['time'], format='%Y/%m/%d')
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        df.to_csv(path, encoding='utf-8', index=False)

    while (1):
        try:
            r = se.get(CURRENT_DATA_URL, headers=JGB_HEADERS)
            df = pd.read_csv(StringIO(r.text), header=1)
            break
        except Exception as e:
            print(e)
            time.sleep(10)

    df.replace("-", np.nan, inplace=True)
    df.rename(columns={'Date':'time'}, inplace=True)
    df['time'] = pd.to_datetime(df['time'], format='%Y/%m/%d')
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

    old_df = pd.read_csv(path)
    old_df = pd.concat([old_df, df], axis=0)
    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True) # last
    old_df['time'] = pd.to_datetime(old_df['time'], format='%Y-%m-%d')
    old_df.sort_values(by='time', axis=0, ascending=True, inplace=True)
    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
    old_df.to_csv(path, encoding='utf-8', index=False)


if __name__=="__main__":
    update_jgb_rate()

    pass

