import os
import time
import numpy as np
import pandas as pd
import datetime
import requests
from utils import *
from cftc import *
from chinamoney import *
from sgx_fut_opt import plot_sgx_option_strike_volume_oi


sina_fx_code_name = {
    'fx_susdcnh': 'USDCNH', 
    'fx_susdcny': 'USDCNY',
    'fx_seurcny': 'EURCNY',
    'fx_saudcny': 'AUDCNY',
    'fx_shkdcny': 'HKDCNY',
    'fx_scnyjpy': 'CNYJPY',

    'DINIW': 'USDIDX',  # 美元指数
    'fx_seurusd': 'EURUSD',
    'fx_sgbpusd': 'GBPUSD',
    'fx_susdjpy': 'USDJPY',
    'fx_saudusd': 'AUDUSD',
    'fx_susdchf': 'USDCHF',
    'fx_susdcad': 'USDCAD',
    'fx_snzdusd': 'NZDUSD',
    'fx_susdhkd': 'USDHKD',
    'fx_susdrub': 'USDRUB',
    'fx_susdkrw': 'USDKRW',
    'fx_susdthb': 'USDTHB',
    'fx_susdsgd': 'USDSGD',
    'fx_susdtry': 'USDTRY',
    'fx_susdbrl': 'USDBRL',
    'fx_susdmxn': 'USDMXN',
    'fx_susdars': 'USDARS',  # 阿根廷
    'fx_susdzar': 'USDZAR',  # 南非

    # 交叉汇率
    'fx_seurgbp': 'EURGBP',
    'fx_seurjpy': 'EURJPY',
    'fx_saudjpy': 'AUDJPY',
    'fx_sgbpcad': 'GBPCAD',
}

headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "vip.stock.finance.sina.com.cn",
    "Proxy-Connection": "keep-alive",
    'Sec-Fetch-Site': 'same-site',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
}

def update_fx_data():
    # INVESTTNG_FX_URL example
    # INVESTTNG_FX_URL = 'https://api.investing.com/api/financialdata/historical/1?start-date=2023-07-04&end-date=2023-08-05&time-frame=Daily&add-missing-rows=false'

    se = requests.session()
    INVESTTNG_FX_URL = 'https://api.investing.com/api/financialdata/historical/{}?start-date={}&end-date={}&time-frame=Daily&add-missing-rows=false'
    SINA_FX_URL = 'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/data=/NewForexService.getDayKLine?symbol={}'

    for code in sina_fx_code_name:
        name = sina_fx_code_name[code]
        print(name)
        path = os.path.join(fx_dir, name+'.csv')

        url = SINA_FX_URL.format(code)
        r = se.get(url, verify=False, headers=headers)
        s = r.text
        w1 = s.find('(')
        w2 = s.find(')')
        z = (s[w1+2:w2-1]).split('|')

        data = []
        for tolhc in z:
            data.append(tolhc.split(',')[:5])

        df = pd.DataFrame(columns=['time','open','low','high','close'], data=data)
        df = df[['time','open','high','low','close']]
        df.to_csv(path, encoding='utf-8', index=False)



cftc_name_code = {
    'USDIDX': '098662',
    'EURUSD': '099741',
    'USDJPY': '097741',
    'AUDUSD': '232741',
    'USDCAD': '090741',
    'GBPUSD': '096742',
    'NZDUSD': '112741',
    'USDBRL': '102741',
    'USDMXN': '095741',
    'EURGBP': '299741',
    'USDZAR': '122741',
}



def plot_fx_position(name):
    start_time = '2006-1-1'
    end_time = '2099-12-31'

    path = os.path.join(fx_dir, name+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['close'], dtype=float)

    t, data = get_period_data(t, data, start_time, end_time, remove_nan=True)

    code = cftc_name_code[name]
    cftc_plot_financial(t, data, name, code=code, inst_name=name)


def plot_all_fx_position():
    for name in cftc_name_code:
        plot_fx_position(name)


def plot_cny():
    path = os.path.join(fx_dir, 'USDCNY'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    usdcny_open = np.array(df['open'], dtype=float)
    usdcny_close = np.array(df['close'], dtype=float)

    path = os.path.join(fx_dir, 'CNY中间价'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    usdcny_middle = np.array(df['USDCNY'], dtype=float)

    path = os.path.join(fx_dir, 'CNY FORWARD'+'.csv') 
    df = pd.read_csv(path)
    t3 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    usdcny_forward_3m = np.array(df['USDCNY_3M'], dtype=float)
    usdcny_forward_6m = np.array(df['USDCNY_6M'], dtype=float)

    t4, diff = data_sub(t2, usdcny_middle, t1, usdcny_open)
    t5, diff1 = data_sub(t3, usdcny_forward_3m, t1, usdcny_close)
    t6, diff2 = data_sub(t3, usdcny_forward_6m, t1, usdcny_close)

    datas = [
             [[[t1,usdcny_open,'USDCNY OPEN','color=red'],
               [t1,usdcny_close,'USDCNY CLOSE','color=black'],
               [t2,usdcny_middle,'中间价','color=blue'],
              ],
              [[t4,diff,'中间价 - 开盘价','style=vbar'],],''],

             [[[t1,usdcny_close,'USDCNY CLOSE','color=black'],
               [t3,usdcny_forward_3m,'USDCNY 3M FORWRD','color=orange'],
              ],
              [[t5,diff1,'3M - CLOSE','style=vbar'],],''],

             [[[t1,usdcny_close,'USDCNY CLOSE','color=black'],
               [t3,usdcny_forward_6m,'USDCNY 6M FORWRD','color=blue'],
              ],
              [[t6,diff2,'6M - CLOSE','style=vbar'],],''],

             [[[t1,usdcny_close,'USDCNY CLOSE','color=black'],
               [t3,usdcny_forward_3m,'USDCNY 3M FORWRD','color=orange'],
               [t3,usdcny_forward_6m,'USDCNY 6M FORWRD','color=blue'],
              ],
              [],''],
    ]
    plot_many_figure(datas, start_time='2015-08-11')


if __name__=="__main__":
    # 各个货币对的历史日线数据
    # update_fx_data()

    # # CNY 中间价
    # update_cny_middle()
    # # CNY 远期
    # update_cny_forward()

    plot_cny()
    # SGX USDCNH
    plot_sgx_option_strike_volume_oi('UC')

    plot_fx_position('USDIDX')
    plot_fx_position('EURUSD')
    plot_fx_position('AUDUSD')
    plot_fx_position('USDJPY')
    # plot_all_fx_position()





    pass
