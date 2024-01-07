import os
import pandas as pd
import datetime
import numpy as np
import requests
import bs4
from utils import *
from io import StringIO, BytesIO


def update_lbma_price():
    se = requests.session()
    # URL = 'https://prices.lbma.org.uk/json/gold_pm.json'
    URL = 'https://prices.lbma.org.uk/json/{}.json'
    LBMA_HEADERS = {
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "prices.lbma.org.uk",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    variety = 'gold'
    for variety in ['gold', 'silver', 'platinum', 'palladium']:
        df = pd.DataFrame()
        if variety != 'silver':
            zs = ['_am', '_pm']
        else:
            zs = ['']
        for z in zs:
            url = URL.format(variety + z)
            while (1):
                try:
                    print(variety + z)
                    r = se.get(url, headers=LBMA_HEADERS)
                    data_json = r.json()
                    break
                except Exception as e:
                    print(e)
                    time.sleep(10)

            temp_df = pd.DataFrame(data_json)
            v = temp_df['v'].values.tolist()
            temp_df = temp_df[['d']]
            temp_df[['USD'+z.upper(), 'GBP'+z.upper(), 'EUR'+z.upper()]] = v
            temp_df.rename(columns={'d':'time'}, inplace=True)

            if (df.empty):
                df = temp_df.copy()
            else:
                df = pd.merge(df, temp_df, on='time', how='outer')

        path = os.path.join(lbma_dir, variety+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            old_df = pd.concat([old_df, df], axis=0)
            old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            old_df.sort_values(by = 'time', inplace=True)
            old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            old_df.to_csv(path, encoding='utf-8', index=False)
        else:
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
            df.sort_values(by = 'time', inplace=True)
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            df.to_csv(path, encoding='utf-8', index=False)


def to_month_end(x):
    dt = datetime.datetime.fromtimestamp(x/1000)
    month_end_day_dt = get_month_last_day(dt.year, dt.month)
    month_end_day = month_end_day_dt.strftime('%Y-%m-%d')
    return month_end_day


def update_lbma_vault_data():
    se = requests.session()
    url = 'https://www.lbma.org.uk/vault-holdings-data/data.json'
    LBMA_HEADERS = {
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.lbma.org.uk",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    while (1):
        try:
            print('LBMA VAULT')
            r = se.get(url, headers=LBMA_HEADERS)
            data_json = r.json()
            break
        except Exception as e:
            print(e)
            time.sleep(10)

    df = pd.DataFrame(data_json)
    df.columns = ['time', 'gold', 'silver']
    df['time'] = df['time'].apply(lambda x:to_month_end(x))


    path = os.path.join(lbma_dir, 'london_vault'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        old_df.sort_values(by = 'time', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        df.sort_values(by = 'time', inplace=True)
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        df.to_csv(path, encoding='utf-8', index=False)



def update_lbma_clearing_data():
    se = requests.session()
    url = 'https://www.lbma.org.uk/clearing-data/data.json'
    LBMA_HEADERS = {
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Host": "www.lbma.org.uk",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    while (1):
        try:
            print('LBMA CLEARING DATA')
            r = se.get(url, headers=LBMA_HEADERS)
            data_json = r.json()
            break
        except Exception as e:
            print(e)
            time.sleep(10)

    df = pd.DataFrame(data_json)
    df.columns = ['time', 'gold_volume_transfered', 'gold_number_of_transferes', 'gold_value_transfered', 
                          'silver_volume_transfered', 'silver_number_of_transferes', 'silver_value_transfered',]
    df['time'] = df['time'].apply(lambda x:to_month_end(x))

    path = os.path.join(lbma_dir, 'clearing_data'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        old_df.sort_values(by = 'time', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        df.sort_values(by = 'time', inplace=True)
        df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        df.to_csv(path, encoding='utf-8', index=False)


def plot_lbma_vault_data(variety):
    path = os.path.join(lbma_dir, 'london_vault'+'.csv')
    t1, d1 = read_csv_data(path, [variety])

    path = os.path.join(lbma_dir, variety+'.csv')
    if variety == 'silver':
        col = 'USD'
    else:
        col = 'USD_PM'
    t2, d2 = read_csv_data(path, [col])

    datas = [[[[t2, d2[col], variety, 'color=black'],
               ],
              [[t1, d1[variety], variety+' lodon vault', 'color=blue'],],''],
    ]
    plot_many_figure(datas, start_time='2016-01-01')


if __name__=="__main__":
    # update_lbma_price()
    # update_lbma_vault_data()
    # update_lbma_clearing_data()

    plot_lbma_vault_data('gold')
    plot_lbma_vault_data('silver')

