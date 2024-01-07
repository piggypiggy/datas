import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
import json


# 农业农村部


MOA_NAME_CODE = {
    '花生': 'AB01001',
    '大豆': 'AA02001',
    '玉米淀粉': 'AA010090002',
}

MOA_NAME = [['大豆', '压榨毛利润'], 
            ['花生' ,'压榨毛利润'], ['花生' ,'压榨开工率'],
            ['玉米淀粉', '开工率'],]


# 毛利润 开工率
def update_moa_profit_production_data():
    se = requests.session()
    MOA_HEADERS = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Host": "ncpscxx.moa.gov.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        }
    PROFIT_URL = 'http://ncpscxx.moa.gov.cn/product/food-cost-press/getFoodConsumptionTrend?queryStartTime={}&queryEndTime={}&varietyCode={}'
    PRODUCTION_URL = 'http://ncpscxx.moa.gov.cn/product/food-process-operating/getOperatingRate?varietyCode={}&queryStartTime={}&queryEndTime={}'

    earlist_time = '2018-01-01'

    now = datetime.datetime.now()
    for name in MOA_NAME:
        if not(name[0] in MOA_NAME_CODE):
            continue

        path = os.path.join(spot_dir, name[0]+name[1]+'.csv')
        if os.path.exists(path):
            old_df = pd.read_csv(path)
            t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
            start_time_dt = t[-1] + pd.Timedelta(days=1)
        else:
            start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')
        start_time = start_time_dt.strftime('%Y-%m-%d')
        end_time = now.strftime('%Y-%m-%d')

        if ('毛利润' in name[1]):
            url = PROFIT_URL.format(start_time, end_time, MOA_NAME_CODE[name[0]])
        elif ('开工率' in name[1]):
            url = PRODUCTION_URL.format(MOA_NAME_CODE[name[0]], start_time, end_time)
        else:
            return

        while (1):
            try:
                print(name[0] + ' ' + name[1] + ' ' + start_time + ' - ' + end_time)
                r = se.get(url, verify=False, headers=MOA_HEADERS)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        data_json = r.json()
        df = pd.DataFrame(columns=['time', name[1]])
        if ('毛利润' in name[1]):
            df['time'] = data_json['data']['x']
            df[name[1]] = data_json['data']['pricePress']

        if ('开工率' in name[1]):
            df['time'] = data_json['data']['x']
            df[name[1]] = data_json['data']['y']
            df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y年%m月%d日'))
            df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))


        if os.path.exists(path):
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



# 开工率 屠宰后均重
def update_moa_pork_data():
    se = requests.session()
    MOA_HEADERS = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Host": "ncpscxx.moa.gov.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        }
    WEIGHT_URL = 'http://ncpscxx.moa.gov.cn/product/livestock-process-weight/weight/slaught/trend/count?varietyCode=AL01001&queryStartTime={}&queryEndTime={}'
    PRODUCTION_URL = 'http://ncpscxx.moa.gov.cn/product/livestock-process-operating/nationwide/days/rate/count?&varietyCode=AL01001&queryStartTime={}&queryEndTime={}'

    earlist_time = '2018-01-01'

    now = datetime.datetime.now()

    path = os.path.join(spot_dir, '生猪加工'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] - pd.Timedelta(days=10)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')
    start_time = start_time_dt.strftime('%Y-%m-%d')
    end_time = now.strftime('%Y-%m-%d')

    df = pd.DataFrame()
    for name in ['生猪屠宰开工率', '屠宰后均重']:
        if (name == '生猪屠宰开工率'):
            url = PRODUCTION_URL.format(start_time, end_time)
        elif (name == '屠宰后均重'):
            url = WEIGHT_URL.format(start_time, end_time)
        else:
            return

        while (1):
            try:
                print(name + ' ' + start_time + ' - ' + end_time)
                r = se.get(url, verify=False, headers=MOA_HEADERS)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        data_json = r.json()
        temp_df = pd.DataFrame(data_json['data'])
        if (name == '生猪屠宰开工率'):
            temp_df['REPORT_TIME'] = temp_df['REPORT_TIME'].apply(lambda x:pd.to_datetime(x, format='%Y年%m月%d日'))
            temp_df['REPORT_TIME'] = temp_df['REPORT_TIME'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            temp_df.rename(columns={'REPORT_TIME':'time', 'OPERATING_RATE':'屠宰开工率'}, inplace=True)
            temp_df = temp_df[['time', '屠宰开工率']]   

        if (name == '屠宰后均重'):
            temp_df['REPORT_TIME'] = temp_df['REPORT_TIME'].apply(lambda x:pd.to_datetime(x, format='%Y年%m月%d日'))
            temp_df['REPORT_TIME'] = temp_df['REPORT_TIME'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
            temp_df.rename(columns={'REPORT_TIME':'time', 'WEIGHT_SLAUGHT':'屠宰后均重'}, inplace=True)
            temp_df = temp_df[['time', '屠宰后均重']]

        if (df.empty):
            df = temp_df.copy()
        else:
            df = pd.merge(df, temp_df, on='time', how='outer')

    if os.path.exists(path):
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


# 棉花加工品价格
def update_cotton_process_production_price():
    se = requests.session()
    MOA_HEADERS = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Host": "ncpscxx.moa.gov.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        }
    URL = 'http://ncpscxx.moa.gov.cn/product/common-price-avg/meat/price/compared/count?dataSource=1&varietyCode=AC010010010,AC010010009,AC010010011,AC010010012,AC010010013&queryStartTime={}&queryEndTime={}'
    earlist_time = '2017-01-01'
    now = datetime.datetime.now()

    path = os.path.join(spot_dir, '棉花加工品价格'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        t = pd.DatetimeIndex(pd.to_datetime(old_df['time'], format='%Y-%m-%d'))
        start_time_dt = t[-1] + pd.Timedelta(days=1)
    else:
        start_time_dt = pd.to_datetime(earlist_time, format='%Y-%m-%d')
    start_time = start_time_dt.strftime('%Y-%m-%d')
    end_time = now.strftime('%Y-%m-%d')

    url = URL.format(start_time, end_time)

    while (1):
        try:
            print('棉花加工品价格 ' + start_time + ' - ' + end_time)
            r = se.get(url, verify=False, headers=MOA_HEADERS)
            data_json = r.json()
            df = pd.DataFrame(data_json['data'])
            break
        except Exception as e:
            print(e)
            time.sleep(5)

    df.rename(columns={'REPORT_TIME':'time', 'C_AC010010009':'棉纱', 
                       'C_AC010010013':'棉壳', 'C_AC010010010':'棉短绒', 'C_AC010010011':'棉粕', 
                       'C_AC010010012':'棉油', }, inplace=True)
    df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y年%m月%d日'))
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))

    df = df[['time', '棉纱', '棉油', '棉短绒', '棉粕', '棉壳']]

    if os.path.exists(path):
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


if __name__=="__main__":
    # # 开工率 毛利润
    # update_moa_profit_production_data()

    # # 生猪加工
    # update_moa_pork_data()

    # 棉花加工品价格
    # update_cotton_process_production_price()


    pass
