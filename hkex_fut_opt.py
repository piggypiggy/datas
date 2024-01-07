import os
import requests
import re
import pandas as pd
import zipfile
import datetime
import numpy as np
import akshare as ak
from utils import *
import math as m
from io import StringIO, BytesIO


def get_hkex_stock_option_contract_size():
    url = 'https://www.hkex.com.hk/Products/Listed-Derivatives/Single-Stock/Stock-Options?sc_lang=en'
    HKCE_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.hkex.com.hk'}

    se = requests.session()
    r = se.get(url, headers=HKCE_HEADERS)

    df1 = pd.read_html(r.text, attrs={'class_':"table migrate;"})[0]
    df1 = df1[['SEHK Code', 'HKATS Code', 'Contract Size (shares)']]
    df1['Number of Board Lots'] = 1

    df2 = pd.read_html(r.text, attrs={'width':"100%"})[0]
    df2 = df2[['SEHK Code', 'HKATS Code', 'Contract Size (shares)', 'Number of Board Lots']]


    df = pd.concat([df1, df2], axis=0)
    path = os.path.join(option_price_dir, 'hkex', 'contract size'+'.csv')
    df.to_csv(path, encoding='utf-8', index=False) 


def update_hkex_stock_option_expiry_date():
    url = 'https://www.hkex.com.hk/Services/Trading/Derivatives/Overview/Trading-Calendar-and-Holiday-Schedule?sc_lang=en'
    HKCE_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.hkex.com.hk'}

    se = requests.session()
    r = se.get(url, headers=HKCE_HEADERS)
    df = pd.read_html(r.text, attrs={'class_':"table migrate"})[0]
    df.rename(columns={"Last Trading Day / Expiry Day":"Expiry Day",}, inplace=True)

    for i in range(len(df)):
        contract_month = df.loc[i, 'Contract Month']
        month = contract_month[:3]
        year = '20'+contract_month[4:6]
        month = month_dict[month.upper()]
        contract_month_tm = year+'-'+month
        df.loc[i, 'Contract Month'] = contract_month_tm

        expiry_date = df.loc[i, 'Expiry Day'].split('-')
        day = expiry_date[0]
        month = expiry_date[1]
        year = '20'+expiry_date[2]
        month = month_dict[month.upper()]
        expiry_date_tm = datetime.datetime(year=int(year), month=int(month), day=int(day)).strftime('%Y-%m-%d')
        df.loc[i, 'Expiry Day'] = expiry_date_tm

        Settlement_date = df.loc[i, 'Final Settlement Day'].split('-')
        day = Settlement_date[0]
        month = Settlement_date[1]
        year = '20'+Settlement_date[2]
        month = month_dict[month.upper()]
        Settlement_date_tm = datetime.datetime(year=int(year), month=int(month), day=int(day)).strftime('%Y-%m-%d')
        df.loc[i, 'Final Settlement Day'] = Settlement_date_tm

    path = os.path.join(option_price_dir, 'hkex', 'stock option expiry date'+'.csv')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df = pd.concat([old_df, df], axis=0)
        old_df.drop_duplicates(subset=['Expiry Day'], keep='last', inplace=True)
        old_df['Expiry Day'] = old_df['Expiry Day'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d'))
        old_df.sort_values(by = 'Expiry Day', inplace=True)
        old_df['Expiry Day'] = old_df['Expiry Day'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
        old_df.to_csv(path, encoding='utf-8', index=False)
    else:
        df.to_csv(path, encoding='utf-8', index=False)  


def update_hkex_stock_option():
    se = requests.session()
    earlist_time = '2023-01-01 00:00:00'
    # URL = 'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/dqe230907.zip'
    URL = 'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/dqe{}.zip'
    HKEX_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.hkex.com.hk'}

    path = os.path.join(option_price_dir, 'hkex', 'TCH'+'_info'+'.csv')
    last_line_time = get_last_line_time(path, '', earlist_time, 19, '%Y-%m-%d %H:%M:%S')
    data_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d %H:%M:%S')
    start_time_dt = data_time_dt + pd.Timedelta(days=1)
    current_time_dt = datetime.datetime.now()

    while start_time_dt <= current_time_dt:
        t = datetime.datetime(year=start_time_dt.year, month=start_time_dt.month, day=start_time_dt.day, hour=16, minute=0, second=0 )
        start_time = start_time_dt.strftime('%Y-%m-%d')
        start_time_ = start_time_dt.strftime('%Y%m%d')
        url = URL.format(start_time_[2:])
        print(url)

        start_time_dt = start_time_dt + pd.Timedelta(days=1)

        count = 0
        while (1):
            try:
                r = se.get(url,  headers=HKEX_HEADERS, timeout=10)
                if r.status_code == 200:
                    break
                if r.status_code == 404:
                    count = count + 1
                    if count >= 2:
                        break
            except Exception as e:
                print('HK STOCK OPTION ERROR: ', e)
                time.sleep(5)
        
        if count >= 2:
            print(start_time, " 404 NOT FOUND")
            continue

        # TMP
        # path = os.path.join(data_dir, 'dqe230907.zip')
        # TMP

        with zipfile.ZipFile(BytesIO(r.content), "r") as z:
        # with zipfile.ZipFile(path, "r") as z:
            # print(z.namelist())
            file_name = z.namelist()[0]
            f = z.open(file_name, mode='r')
            variety_dict = {}
            while (1):
                s = f.readline().decode('utf-8')
                if 'End of Report' in s:
                    break


                # SUMMARY
                if 'HKATS' in s:
                    a = s.split(',')
                    # EACH STOCK'S SUMMARY
                    while (1):
                        s = f.readline().decode('utf-8')
                        a = s.split(',')
                        L = len(a)
                        if L < 5:
                            break

                        a[L-1] = a[L-1].removesuffix('\r\n')
                        volume = float(a[3])
                        oi = float(a[6])
                        # 只要有成交量和持仓量的股票数据
                        if (volume >= 1000) and (oi >= 15000):
                            variety_dict[a[0]] = 1
                    print(variety_dict)

                # OPTION DATA
                if 'CLASS' in s:
                    inst_ids_list = []
                    a = s.split(',')
                    L = len(a)
                    a[L-1] = a[L-1].removesuffix('"\r\n')
                    a[L-1] = a[L-1].removeprefix('"CLOSING PRICE HK$')
                    close = float(a[L-1])
                    b = a[0].split(' ')
                    variety = b[1]
                    if not(variety in variety_dict):
                        continue

                    # print(variety, close)

                    # skip one line
                    f.readline()

                    # read option data
                    data_dict = {}
                    while (1):
                        s = f.readline().decode('utf-8')
                        a = s.split(',')
                        L = len(a)
                        if L < 5:
                            break
                        a[L-1] = a[L-1].removesuffix('\r\n')

                        month = month_dict[a[0][:3]]
                        year = a[0][3:5]
                        tm = year+month
                        inst_id = variety + tm

                        otype = a[2]
                        strike = a[1]
                        if not (inst_id in data_dict):
                            inst_ids_list.append(inst_id)
                            data_dict[inst_id] = [['time', 'close'],
                                                ['time', 'close'],
                                                ['time', 'close'],
                                                [t, close],
                                                [0.0, 0.0],]

                        data_dict[inst_id][0] = data_dict[inst_id][0] + [otype, otype, otype, otype]
                        data_dict[inst_id][1] = data_dict[inst_id][1] + [strike, strike, strike, strike]
                        data_dict[inst_id][2] = data_dict[inst_id][2] + ['settle', 'volume', 'oi', 'imp_vol']
                        iv = a[8]
                        if iv == '0':
                            iv = np.nan
                        else:
                            iv = float(iv)/100
                        data_dict[inst_id][3] = data_dict[inst_id][3] + [a[6], a[9], a[10], iv]
                        data_dict[inst_id][4][0] = data_dict[inst_id][4][0] + float(a[9])
                        data_dict[inst_id][4][1] = data_dict[inst_id][4][1] + float(a[10])

                    # skip 2 lines
                    f.readline()
                    f.readline()

                    while (1):
                        s = f.readline().decode('utf-8')
                        a = s.split(',')
                        L = len(a)
                        if L < 5:
                            break
                        a[L-1] = a[L-1].removesuffix('\r\n')

                        month = month_dict[a[0][:3]]
                        year = a[0][3:5]
                        tm = year+month
                        inst_id = variety + tm

                        otype = a[2]
                        strike = a[1]
                        if not (inst_id in data_dict):
                            inst_ids_list.append(inst_id)
                            data_dict[inst_id] = [['time', 'close'],
                                                ['time', 'close'],
                                                ['time', 'close'],
                                                [t, close]]

                        data_dict[inst_id][0] = data_dict[inst_id][0] + [otype, otype, otype, otype]
                        data_dict[inst_id][1] = data_dict[inst_id][1] + [strike, strike, strike, strike]
                        data_dict[inst_id][2] = data_dict[inst_id][2] + ['settle', 'volume', 'oi', 'imp_vol']
                        iv = a[8]
                        if iv == '0':
                            iv = np.nan
                        else:
                            iv = float(iv)/100
                        data_dict[inst_id][3] = data_dict[inst_id][3] + [a[6], a[9], a[10], iv]
                        data_dict[inst_id][4][0] = data_dict[inst_id][4][0] + float(a[9])
                        data_dict[inst_id][4][1] = data_dict[inst_id][4][1] + float(a[10])

                    # skip 2 lines
                    f.readline()
                    f.readline()

                    # sort volume
                    inst_ids = ''
                    z1 = []
                    z2 = []
                    for inst_id in data_dict:
                        z1.append(inst_id)
                        z2.append(data_dict[inst_id][4][0])

                    z1 = np.array(z1)
                    z2 = np.array(z2)
                    sort = np.argsort(z2)[::-1]
                    inst_ids = ''
                    for i in range(len(z1)):
                        inst_ids += z1[sort][i]
                        inst_ids += ','

                    for inst_id in inst_ids_list:
                        # inst_ids += inst_id
                        # inst_ids += ','
                        df = pd.DataFrame(columns=[data_dict[inst_id][0], data_dict[inst_id][1], data_dict[inst_id][2]], data=[data_dict[inst_id][3]])
                        path = os.path.join(option_price_dir, 'hkex', inst_id+'.csv')
                        if os.path.exists(path):
                            old_df = pd.read_csv(path, header=[0,1,2])
                            old_df = pd.concat([old_df, df], axis=0)
                            old_df.drop_duplicates(subset=[('time','time','time')], keep='last', inplace=True) # last
                            old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                            old_df.sort_values(by = ('time','time','time'), inplace=True)
                            old_df.loc[:, pd.IndexSlice['time','time','time']] = old_df.loc[:, pd.IndexSlice['time','time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                            old_df.to_csv(path, encoding='utf-8', index=False)
                        else:
                            df.to_csv(path, encoding='utf-8', index=False)

                    info_df = pd.DataFrame(columns=['time', 'inst_ids'])
                    info_df.loc[0, 'time'] = t
                    info_df.loc[0, 'inst_ids'] = inst_ids
                    path = os.path.join(option_price_dir, 'hkex', variety+'_info'+'.csv')
                    # print(df)
                    if os.path.exists(path):
                        old_df = pd.read_csv(path)
                        old_df = pd.concat([old_df, info_df], axis=0)
                        old_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
                        old_df['time'] = old_df['time'].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                        old_df.sort_values(by = 'time', inplace=True)
                        old_df['time'] = old_df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                        old_df.to_csv(path, encoding='utf-8', index=False)
                    else:
                        info_df.to_csv(path, encoding='utf-8', index=False)  


def update_hkex_usdcnh():
    variety = 'USDCNH'
    se = requests.session()
    earlist_time = '2023-01-01 00:00:00'
    URL = 'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/cusf{}.zip'
    HKEX_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    'Host': 'www.hkex.com.hk'}

    path = os.path.join(future_price_dir, 'hkex', 'USDCNH'+'.csv')
    last_line_time = get_last_line_time(path, '', earlist_time, 19, '%Y-%m-%d %H:%M:%S')
    data_time_dt = pd.to_datetime(last_line_time, format='%Y-%m-%d %H:%M:%S')
    start_time_dt = data_time_dt + pd.Timedelta(days=1)
    current_time_dt = datetime.datetime.now()

    while start_time_dt <= current_time_dt:
        # 4:30 pm
        t = datetime.datetime(year=start_time_dt.year, month=start_time_dt.month, day=start_time_dt.day, hour=16, minute=30, second=0 )
        start_time = start_time_dt.strftime('%Y-%m-%d')
        start_time_ = start_time_dt.strftime('%Y%m%d')
        url = URL.format(start_time_[2:])
        print('USDCNH', url)

        start_time_dt = start_time_dt + pd.Timedelta(days=1)

        count = 0
        while (1):
            try:
                r = se.get(url,  headers=HKEX_HEADERS, timeout=10)
                if r.status_code == 200:
                    break
                if r.status_code == 404:
                    count = count + 1
                    if count >= 2:
                        break
            except Exception as e:
                print('HK STOCK OPTION ERROR: ', e)
                time.sleep(5)
        
        if count >= 2:
            print(start_time, " 404 NOT FOUND")
            continue


        with zipfile.ZipFile(BytesIO(r.content), "r") as z:
            file_name = z.namelist()[0]
            f = z.open(file_name, mode='r')
            while (1):
                s = f.readline().decode('utf-8')
                if 'All Contracts Total' in s:
                    break

                if 'Contract Month' in s:
                    # skip one line
                    f.readline()

                    n = 0
                    col1 = ['time']
                    col2 = ['time']
                    data = [t]
                    dom_data = []
                    maxv = -1
                    while (1):
                        s = f.readline().decode('utf-8')
                        a = s.split(',')
                        L = len(a)
                        if L < 5:
                            break
                        a[L-1] = a[L-1].removesuffix('\r\n')

                        if a[1] == 'EXPIRED':
                            continue

                        if (a[1] == '-') and (a[6] == '-'):
                            break

                        month = month_dict[a[0][:3]]
                        year = a[0][4:6]
                        tm = year+month
                        inst_id = variety + tm

                        # c1 - c9
                        n = n + 1
                        cn = 'c'+str(n)
                        if n > 9:
                            break

                        # inst_id, open, high low, settle, volume, oi
                        col1 += [cn, cn, cn, cn, cn, cn, cn]
                        col2 += ['inst_id', 'open', 'high', 'low', 'settle', 'volume', 'oi']
                        data += [inst_id, a[6], a[7], a[8], a[10], a[9], a[15]]
                        volume = float(a[9])
                        if volume > maxv:
                            maxv = volume
                            dom_data = [inst_id, a[6], a[7], a[8], a[10], a[9], a[15]]

                    cn = 'dom'
                    col1 += [cn, cn, cn, cn, cn, cn, cn]
                    col2 += ['inst_id', 'open', 'high', 'low', 'settle', 'volume', 'oi']
                    data += dom_data

                    df = pd.DataFrame(columns=[col1, col2], data=[data])
                    path = os.path.join(future_price_dir, 'hkex', variety+'.csv')
                    if os.path.exists(path):
                        old_df = pd.read_csv(path, header=[0,1])
                        old_df = pd.concat([old_df, df], axis=0)
                        old_df.drop_duplicates(subset=[('time','time')], keep='last', inplace=True) # last
                        old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))
                        old_df.sort_values(by = ('time','time'), inplace=True)
                        old_df.loc[:, pd.IndexSlice['time','time']] = old_df.loc[:, pd.IndexSlice['time','time']].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
                        old_df.to_csv(path, encoding='utf-8', index=False)
                    else:
                        df.to_csv(path, encoding='utf-8', index=False)

                    break


def get_hk_stock_index():
    code_list = [['HSI', '恒生指数'],
                  ['HSCEI', '恒生中国企业指数'],
                  ['HSTECH', '恒生科技指数'],
                  ]

    for code in code_list:
        df = ak.stock_hk_index_daily_sina(symbol=code[0])
        df.rename(columns={"date":"time"}, inplace=True)

        path = os.path.join(data_dir, code[1]+'.csv')
        df.to_csv(path, encoding='utf-8', index=False)
        print(code[1])


def update_hkex_fut_opt_data():
    # 股票期权
    update_hkex_stock_option()

    # # USDCNH
    update_hkex_usdcnh()

    # 股指
    get_hk_stock_index()


if __name__=="__main__":
    # update_hkex_stock_option_expiry_date()

    update_hkex_fut_opt_data()




