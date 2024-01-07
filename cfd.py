import os
import requests
import pandas as pd
import datetime
import numpy as np
from utils import *
from cftc import *


sina_commodity_code_name = {
    'CL': 'WTI_CFD',
    'OIL': 'BRENT_CFD',
    'DBI': 'DBI_CFD',  # 迪拜原油
    'GAS': 'DIESEL_OIL_CFD',  # 柴油
    'FCPO': 'PALM_OIL_CFD', # 马来西亚棕榈油
    'GASO': 'GASOLINE_CFD',  # 汽油
    'NG': 'NATURAL_GAS_CFD',
    'GC': 'GOLD_CFD',
    'SI': 'SILVER_CFD',
    'S': 'SOYBEAN_CFD',
    'BO': 'SOYBEAN_OIL_CFD',
    'SM': 'SOYBEAN_MEAL_CFD',  # 豆粕
    'C': 'CORN_CFD',
    'W': 'WHEAT_CFD',
    'CAD': 'COPPER_CFD',
    'AHD': 'ALUMINUM_CFD',
    'PBD': 'LEAD_CFD',
    'ZSD': 'ZINC_CFD',
    'NID': 'NICKLE_CFD',
    'SND': 'TIN_CFD',

    'RS': 'SUGAR_CFD',
    'CT': 'COTTON_CFD',
    'FEF': 'IRON_ORE_CFD',
    'ES': 'SP500_CFD',
}

headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "stock2.finance.sina.com.cn",
    "Proxy-Connection": "keep-alive",
    'Sec-Fetch-Site': 'same-site',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}

sina_cn_commodity_code_name = {
    'SC0': 'SC0',
    'AU0': 'AU0',
    'AG0': 'AG0',
    'CU0': 'CU0',
    'AL0': 'AL0',
    'ZN0': 'ZN0',

}

headers2 = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "gu.sina.cn",
    "Proxy-Connection": "keep-alive",
    'Sec-Fetch-Site': 'same-site',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}

# finance.sina.com
def update_commodity_cfd_daily_data():
    se = requests.session()
    SINA_COMMODITY_URL = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_data=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={}'

    for code in sina_commodity_code_name:
        name = sina_commodity_code_name[code]
        print(name)

        url = SINA_COMMODITY_URL.format(code)
        r = se.get(url, verify=False, headers=headers)
        s = r.text
        s=s.replace('},', '')
        s=s.replace('"', '')
        s=s.replace('}]);', '')
        z = s.split('{')[1:]
        datas = []
        for i in range(len(z)):
            data = z[i].split(',')[:7]
            for k in range(len(data)):
                data[k] = data[k].split(':')[1]
            
            datas.append(data)

        path = os.path.join(cfd_dir, name+'.csv')
        df = pd.DataFrame(columns=['time','open','high','low','close','volume','oi'], data=datas)
        df.to_csv(path, encoding='utf-8', index=False)

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


def update_commodity_cfd_intraday_data():
    se = requests.session()
    SINA_COMMODITY_URL = 'https://gu.sina.cn/ft/api/jsonp.php/var%20_data=/GlobalService.getMink?symbol={}&type=5'

    for code in sina_commodity_code_name:
        name = sina_commodity_code_name[code] + '_intraday'
        path = os.path.join(cfd_dir, name+'.csv')
        if not os.path.exists(path):
            continue

        print(name)
        url = SINA_COMMODITY_URL.format(code)
        r = se.get(url, verify=False, headers=headers2)
        s = r.text
        s=s.replace('},', '')
        s=s.replace('"', '')
        s=s.replace('}]);', '')
        z = s.split('{')[1:]
        datas = []
        for i in range(len(z)):
            data = z[i].split(',')[:5]
            data[0] = data[0][2:]
            for k in range(1, len(data)):
                data[k] = data[k].split(':')[1]
            
            datas.append(data)

        df = pd.DataFrame(columns=['time','open','high','low','close'], data=datas)

        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
        old_df.sort_values(by = 'time', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))

        old_df.to_csv(path, encoding='utf-8', index=False)


def update_cn_commodity_cfd_intraday_data():
    se = requests.session()
    SINA_COMMODITY_URL = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/DATA=/InnerFuturesNewService.getFewMinLine?symbol={}&type=5'
                      #   https://stock2.finance.sina.com.cn/futures/api/jsonp.php/DATA=/InnerFuturesNewService.getFewMinLine?symbol=ZN0&type=5
    for code in sina_cn_commodity_code_name:
        name = sina_cn_commodity_code_name[code] + '_intraday'
        path = os.path.join(cfd_dir, name+'.csv')
        if not os.path.exists(path):
            continue

        print(name)
        url = SINA_COMMODITY_URL.format(code)
        r = se.get(url, verify=False, headers=headers)
        s = r.text
        s=s.replace('},', '')
        s=s.replace('"', '')
        s=s.replace('}]);', '')
        z = s.split('{')[1:]
        datas = []
        for i in range(len(z)):
            data = z[i].split(',')[:5]
            data[0] = data[0][2:]
            for k in range(1, len(data)):
                data[k] = data[k].split(':')[1]
            
            datas.append(data)

        df = pd.DataFrame(columns=['time','open','high','low','close'], data=datas)

        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
        old_df.sort_values(by = 'time', inplace=True)
        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))

        old_df.to_csv(path, encoding='utf-8', index=False)



def update_usdcny_intraday():
    se = requests.session()
    url = 'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/DATA=/NewForexService.getMinKline?symbol=fx_susdcny&scale=5&datalen=1440'
    print('USDCNY_intraday')

    path = os.path.join(cfd_dir, 'USDCNY_intraday'+'.csv')
    r = se.get(url, verify=False, headers=headers2)
    s = r.text
    s=s.replace('},', '')
    s=s.replace('"', '')
    s=s.replace('}]);', '')
    z = s.split('{')[1:]
    datas = []
    for i in range(len(z)):
        data = z[i].split(',')[:5]
        data[0] = data[0][2:]
        for k in range(1, len(data)):
            data[k] = data[k].split(':')[1]
        
        datas.append(data)

    df = pd.DataFrame(columns=['time','open','high','low','close'], data=datas)

    old_df = pd.read_csv(path)
    old_df = pd.concat([old_df, df], axis=0)
    old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
    old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
    old_df.sort_values(by = 'time', inplace=True)
    old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
    old_df.to_csv(path, encoding='utf-8', index=False)


if __name__=="__main__":
    update_commodity_cfd_daily_data()
    # update_commodity_cfd_intraday_data()
    # update_cn_commodity_cfd_intraday_data()
    # update_usdcny_intraday()



    pass


