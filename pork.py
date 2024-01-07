import os
import requests
import re
import pandas as pd
import datetime
import numpy as np
import akshare as ak
from utils import *
import json
import scipy.stats as ss
import math as m
from bs4 import BeautifulSoup
from io import StringIO, BytesIO


# 农业农村部
def update_moa_pork_data():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Cache-Control": "no-cache",
        "Host": "www.moa.gov.cn",
        'Upgrade-Insecure-Requests': '1',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    }

    ROOT_URL = 'http://www.moa.gov.cn/ztzl/szcpxx/jdsj/'

    earlist_time = '2021-01'
    path = os.path.join(data_dir, 'pork'+'.csv')
    start_time = get_last_line_time(path, 'MOA PORK DATA', earlist_time, 7, '%Y-%m')
    start_time_dt = pd.to_datetime(start_time, format='%Y-%m')
    current_time = datetime.datetime.now().strftime('%Y-%m')
    current_time_dt = pd.to_datetime(current_time, format='%Y-%m')

    se = requests.session()
    while (start_time_dt < current_time_dt):
        start_time = start_time_dt.strftime('%Y%m')
        # 'http://www.moa.gov.cn/ztzl/szcpxx/jdsj/2022/202211/'
        if ((start_time_dt.year == 2021) and (start_time_dt.month < 12)):
            url = ROOT_URL + start_time + '/'
        else:
            url = ROOT_URL + start_time[:4] + '/' + start_time + '/'
        print(url)

        if start_time_dt.month == 12:
            start_time_dt = pd.to_datetime(datetime.datetime(start_time_dt.year + 1, 1, 1))
        else:
            start_time_dt = pd.to_datetime(datetime.datetime(start_time_dt.year, start_time_dt.month + 1, 1))

        try:
            r = se.get(url, headers=headers)
            soup = BeautifulSoup(r.text, 'html.parser')
            a_tag = soup.find('a', href=True, class_='redBtn') # Find <a> tag that have a href attr

            url = url + a_tag['href']
            print(url)
            r = se.get(url, headers=headers)
            df = pd.read_excel(BytesIO(r.content), header=0)
        except Exception as e:
            print(e)
            continue

        k = 0
        for i in range(len(df)):
            a = (df.loc[i,:] == '同比')
            b = False
            for k in range(len(a)):
                b = b or a[k]
            if b == True:
                break

        for j in range(i+1, len(df)):
            if (np.isnan(df.iloc[j, 1])):
                break

        df1 = df.iloc[i+1:j, 2:]

        name = np.array(df1.iloc[:,0])
        value = np.array(df1.iloc[:,1])
        mom = np.array(df1.iloc[:,2])
        yoy = np.array(df1.iloc[:,3])

        # print(name)
        for i in range(len(name)):
            w = name[i].find('月规模')
            if w >= 0:
                name[i] = '累计' + name[i][w+1:]
                continue

            w = name[i].find('月末')
            if w >= 0:
                name[i] = name[i][w+2:]
                continue

            w = name[i].find('月份')
            if w >= 0:
                name[i] = name[i][w+2:]
                continue

            w = name[i].find('月')
            if w >= 0:
                name[i] = name[i][w+1:]
                continue

            w = name[i].find('季度末')
            if w >= 0:
                name[i] = name[i][w+3:]
                continue

            w = name[i].find('季度')
            if w >= 0:
                name[i] = name[i][w+2:]
                continue

            w = name[i].find('上半年')
            if w >= 0:
                name[i] = name[i][w+3:]
                continue

            w = name[i].find('年')
            if w >= 0:
                name[i] = name[i][w+1:]
                continue

        for i in range(len(value)):
            if (type(value[i]) == str):
                w = value[i].find('（')
                if w >= 0:
                    value[i] = float(value[i][:w])
                else:
                    value[i] = np.nan

        for i in range(len(mom)):
            if (type(mom[i]) == str):
                mom[i] = mom[i].replace('*', '')
                mom[i] = mom[i].replace('%', '')
                try:
                    mom[i] = float(mom[i])
                except:
                    mom[i] = np.nan
            else:
                mom[i] = mom[i] * 100

        for i in range(len(yoy)):
            if (type(yoy[i]) == str):
                yoy[i] = yoy[i].replace('*', '')
                yoy[i] = yoy[i].replace('%', '')
                try:
                    yoy[i] = float(yoy[i])
                except:
                    yoy[i] = np.nan
            else:
                yoy[i] = yoy[i] * 100

        name_list = list(name)
        name_list_mom = [z + '_环比' for z in name_list]
        name_list_yoy = [z + '_同比' for z in name_list]
        cols = ['time'] + name_list + name_list_mom + name_list_yoy
        data = np.append(value, mom)
        data = np.append(data, yoy)
        
        date_str = start_time[0:4]
        date_str += '-'
        date_str += start_time[4:6]
        data = np.append(date_str, data)

        df_new = pd.DataFrame(columns=cols)
        df_new.loc[0] = data
        
        if os.path.exists(path):
            df_old = pd.read_csv(path)
            df_old = pd.concat([df_old, df_new], axis=0)
            df_old.drop_duplicates(subset=['time'], keep='last', inplace=True)
            df_old.to_csv(path, encoding='utf-8', index=False)
        else:
            df_new.to_csv(path, encoding='utf-8', index=False)



def test1():
    path = os.path.join(data_dir, 'pork'+'.csv')
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m'))
    data11 = np.array(df1['能繁母猪存栏（万头）'], dtype=float)
    data12 = np.array(df1['生猪存栏（万头）'], dtype=float)
    data13 = np.array(df1['生猪出栏（万头）'], dtype=float)
    data14 = np.array(df1['规模以上生猪定点屠宰企业屠宰量（万头）'], dtype=float)
    data15 = np.array(df1['全国二元母猪销售价格（元/公斤）'], dtype=float)
    data16 = np.array(df1['全国批发市场白条猪价格（元/公斤）'], dtype=float)
    data17 = np.array(df1['36个大中城市猪肉（精瘦肉）零售价格（元/公斤）'], dtype=float)
    data18 = np.array(df1['规模养殖生猪每头净利润（元）'], dtype=float)
    data19 = np.array(df1['散养生猪每头净利润（元）'], dtype=float)


    path = os.path.join(future_price_dir, 'dce', 'lh_spot.csv')
    df2 = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    data2 = np.array(df2['spot_price'], dtype=float)


    datas = [[[
               [t1, data12, '生猪存栏（万头）', ''],
               [t1, data13, '生猪出栏（万头）', ''],
              ],

              [[t2, data2, '猪肉现货价格','']],'']]
    plot_many_figure(datas)

    datas = [[[
               [t1, data11, '能繁母猪存栏（万头）', ''],
               [t1, data14, '规模以上生猪定点屠宰企业屠宰量（万头）', ''],
              ],

              [[t2, data2, '猪肉现货价格','']],'']]
    plot_many_figure(datas)

    datas = [[[
              [t1, data18, '规模养殖生猪每头净利润（元）',''],
              [t1, data19, '散养生猪每头净利润（元）',''],],

              [[t2, data2, '猪肉现货价格','']],'']]
    plot_many_figure(datas)


# 30个省 猪肉价格变化
def test2():
    path = os.path.join(data_dir, '猪价格'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    cols = df.columns.to_list()
    wai3yuan_cols = []
    nei3yuan_cols = []
    tuza_cols = []
    for col in cols:
        if ('外三元' in col):
            wai3yuan_cols.append(col)
        if ('内三元' in col):
            nei3yuan_cols.append(col)            
        if ('土杂' in col):
            tuza_cols.append(col)

    wai3yuan = np.array(df[wai3yuan_cols], dtype=float)
    nei3yuan = np.array(df[nei3yuan_cols], dtype=float)
    tuza = np.array(df[tuza_cols], dtype=float)

    wai3yuan_hi = []
    wai3yuan_eq = []
    wai3yuan_lo = []
    wai3yuan_avg_price = []
    for i in range(len(wai3yuan)-1):
        w = wai3yuan[i+1] > wai3yuan[i]
        wai3yuan_hi.append(np.sum(wai3yuan[i+1][w]-wai3yuan[i][w]))
        w = wai3yuan[i+1] == wai3yuan[i]
        wai3yuan_eq.append(0)
        w = wai3yuan[i+1] < wai3yuan[i]
        wai3yuan_lo.append(np.sum(wai3yuan[i][w]-wai3yuan[i+1][w]))
        wai3yuan_avg_price.append(np.average(wai3yuan[i+1]))
    wai3yuan_hi = np.array(wai3yuan_hi)
    wai3yuan_eq = np.array(wai3yuan_eq)
    wai3yuan_lo = np.array(wai3yuan_lo)
    wai3yuan_avg_price = np.array(wai3yuan_avg_price) * 1000

    wai3yuan_hi_count = []
    wai3yuan_eq_count = []
    wai3yuan_lo_count = []
    for i in range(len(wai3yuan)-1):
        wai3yuan_hi_count.append(np.sum(wai3yuan[i+1] > wai3yuan[i]))
        wai3yuan_eq_count.append(np.sum(wai3yuan[i+1] == wai3yuan[i]))
        wai3yuan_lo_count.append(np.sum(wai3yuan[i+1] < wai3yuan[i]))
    wai3yuan_hi_count = np.array(wai3yuan_hi_count)
    wai3yuan_eq_count = np.array(wai3yuan_eq_count)
    wai3yuan_lo_count = np.array(wai3yuan_lo_count)


    nei3yuan_hi = []
    nei3yuan_eq = []
    nei3yuan_lo = []
    nei3yuan_avg_price = []
    for i in range(len(nei3yuan)-1):
        w = nei3yuan[i+1] > nei3yuan[i]
        nei3yuan_hi.append(np.sum(nei3yuan[i+1][w]-nei3yuan[i][w]))
        w = nei3yuan[i+1] == nei3yuan[i]
        nei3yuan_eq.append(0)
        w = nei3yuan[i+1] < nei3yuan[i]
        nei3yuan_lo.append(np.sum(nei3yuan[i][w]-nei3yuan[i+1][w]))
        nei3yuan_avg_price.append(np.average(nei3yuan[i+1]))
    nei3yuan_hi = np.array(nei3yuan_hi)
    nei3yuan_eq = np.array(nei3yuan_eq)
    nei3yuan_lo = np.array(nei3yuan_lo)
    nei3yuan_avg_price = np.array(nei3yuan_avg_price) * 1000

    nei3yuan_hi_count = []
    nei3yuan_eq_count = []
    nei3yuan_lo_count = []
    for i in range(len(nei3yuan)-1):
        nei3yuan_hi_count.append(np.sum(nei3yuan[i+1] > nei3yuan[i]))
        nei3yuan_eq_count.append(np.sum(nei3yuan[i+1] == nei3yuan[i]))
        nei3yuan_lo_count.append(np.sum(nei3yuan[i+1] < nei3yuan[i]))
    nei3yuan_hi_count = np.array(nei3yuan_hi_count)
    nei3yuan_eq_count = np.array(nei3yuan_eq_count)
    nei3yuan_lo_count = np.array(nei3yuan_lo_count)



    tuza_hi = []
    tuza_eq = []
    tuza_lo = []
    tuza_avg_price = []
    for i in range(len(tuza)-1):
        w = tuza[i+1] > tuza[i]
        tuza_hi.append(np.sum(tuza[i+1][w]-tuza[i][w]))
        w = tuza[i+1] == tuza[i]
        tuza_eq.append(0)
        w = tuza[i+1] < tuza[i]
        tuza_lo.append(np.sum(tuza[i][w]-tuza[i+1][w]))
        tuza_avg_price.append(np.average(tuza[i+1]))
    tuza_hi = np.array(tuza_hi)
    tuza_eq = np.array(tuza_eq)
    tuza_lo = np.array(tuza_lo)
    tuza_avg_price = np.array(tuza_avg_price) * 1000

    tuza_hi_count = []
    tuza_eq_count = []
    tuza_lo_count = []
    for i in range(len(tuza)-1):
        tuza_hi_count.append(np.sum(tuza[i+1] > tuza[i]))
        tuza_eq_count.append(np.sum(tuza[i+1] == tuza[i]))
        tuza_lo_count.append(np.sum(tuza[i+1] < tuza[i]))
    tuza_hi_count = np.array(tuza_hi_count)
    tuza_eq_count = np.array(tuza_eq_count)
    tuza_lo_count = np.array(tuza_lo_count)


    T1 = 3
    weights = np.ones(T1)/T1
    wai3yuan_hi_avg1 = np.convolve(wai3yuan_hi, weights)[T1-1:-T1+1]
    wai3yuan_eq_avg1 = np.convolve(wai3yuan_eq, weights)[T1-1:-T1+1]
    wai3yuan_lo_avg1 = np.convolve(wai3yuan_lo, weights)[T1-1:-T1+1]
    wai3yuan_hi_count_avg1 = np.convolve(wai3yuan_hi_count, weights)[T1-1:-T1+1]
    wai3yuan_eq_count_avg1 = np.convolve(wai3yuan_eq_count, weights)[T1-1:-T1+1]
    wai3yuan_lo_count_avg1 = np.convolve(wai3yuan_lo_count, weights)[T1-1:-T1+1]

    nei3yuan_hi_avg1 = np.convolve(nei3yuan_hi, weights)[T1-1:-T1+1]
    nei3yuan_eq_avg1 = np.convolve(nei3yuan_eq, weights)[T1-1:-T1+1]
    nei3yuan_lo_avg1 = np.convolve(nei3yuan_lo, weights)[T1-1:-T1+1]
    nei3yuan_hi_count_avg1 = np.convolve(nei3yuan_hi_count, weights)[T1-1:-T1+1]
    nei3yuan_eq_count_avg1 = np.convolve(nei3yuan_eq_count, weights)[T1-1:-T1+1]
    nei3yuan_lo_count_avg1 = np.convolve(nei3yuan_lo_count, weights)[T1-1:-T1+1]

    tuza_hi_avg1 = np.convolve(tuza_hi, weights)[T1-1:-T1+1]
    tuza_eq_avg1 = np.convolve(tuza_eq, weights)[T1-1:-T1+1]
    tuza_lo_avg1 = np.convolve(tuza_lo, weights)[T1-1:-T1+1]
    tuza_hi_count_avg1 = np.convolve(tuza_hi_count, weights)[T1-1:-T1+1]
    tuza_eq_count_avg1 = np.convolve(tuza_eq_count, weights)[T1-1:-T1+1]
    tuza_lo_count_avg1 = np.convolve(tuza_lo_count, weights)[T1-1:-T1+1]

    t1 = t[1+T1-1:]


    T2 = 10
    weights = np.ones(T2)/T2
    wai3yuan_hi_avg2 = np.convolve(wai3yuan_hi, weights)[T2-1:-T2+1]
    wai3yuan_eq_avg2 = np.convolve(wai3yuan_eq, weights)[T2-1:-T2+1]
    wai3yuan_lo_avg2 = np.convolve(wai3yuan_lo, weights)[T2-1:-T2+1]
    wai3yuan_hi_count_avg2 = np.convolve(wai3yuan_hi_count, weights)[T2-1:-T2+1]
    wai3yuan_eq_count_avg2 = np.convolve(wai3yuan_eq_count, weights)[T2-1:-T2+1]
    wai3yuan_lo_count_avg2 = np.convolve(wai3yuan_lo_count, weights)[T2-1:-T2+1]

    nei3yuan_hi_avg2 = np.convolve(nei3yuan_hi, weights)[T2-1:-T2+1]
    nei3yuan_eq_avg2 = np.convolve(nei3yuan_eq, weights)[T2-1:-T2+1]
    nei3yuan_lo_avg2 = np.convolve(nei3yuan_lo, weights)[T2-1:-T2+1]
    nei3yuan_hi_count_avg2 = np.convolve(nei3yuan_hi_count, weights)[T2-1:-T2+1]
    nei3yuan_eq_count_avg2 = np.convolve(nei3yuan_eq_count, weights)[T2-1:-T2+1]
    nei3yuan_lo_count_avg2 = np.convolve(nei3yuan_lo_count, weights)[T2-1:-T2+1]

    tuza_hi_avg2 = np.convolve(tuza_hi, weights)[T2-1:-T2+1]
    tuza_eq_avg2 = np.convolve(tuza_eq, weights)[T2-1:-T2+1]
    tuza_lo_avg2 = np.convolve(tuza_lo, weights)[T2-1:-T2+1]
    tuza_hi_count_avg2 = np.convolve(tuza_hi_count, weights)[T2-1:-T2+1]
    tuza_eq_count_avg2 = np.convolve(tuza_eq_count, weights)[T2-1:-T2+1]
    tuza_lo_count_avg2 = np.convolve(tuza_lo_count, weights)[T2-1:-T2+1]

    t2 = t[1+T2-1:]

    t = t[1:]

    path2 = os.path.join(future_price_dir, 'dce', 'lh'+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    fut_t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    fut_price = np.array(fut_df['index']['close'], dtype=float)

    datas = [       
             [[[t2, wai3yuan_hi_avg2, '外三元价格 上升 价格变化求和 ' + str(T2) + 'd avg', 'color=red'],
               [t2, wai3yuan_eq_avg2, '外三元价格 不变 价格变化求和 ' + str(T2) + 'd avg', 'color=gray'],
               [t2, wai3yuan_lo_avg2, '外三元价格 下降 价格变化求和 ' + str(T2) + 'd avg', 'color=darkgreen']],
              [],'30个省'],

             [[[t1, wai3yuan_hi_avg1, '外三元价格 上升 价格变化求和 ' + str(T1) + 'd avg', 'color=red'],
               [t1, wai3yuan_eq_avg1, '外三元价格 不变 价格变化求和 ' + str(T1) + 'd avg', 'color=gray'],
               [t1, wai3yuan_lo_avg1, '外三元价格 下降 价格变化求和 ' + str(T1) + 'd avg', 'color=darkgreen']],
              [],''],
             
             [[[t, wai3yuan_hi, '外三元价格 上升 价格变化求和 ', 'color=red'],
               [t, wai3yuan_eq, '外三元价格 不变 价格变化求和 ', 'color=gray'],
               [t, wai3yuan_lo, '外三元价格 下降 价格变化求和 ', 'color=darkgreen']],
              [],''],
             
             [[[fut_t, fut_price, '生猪 指数', 'color=black'],
               [t, wai3yuan_avg_price, '外三元价格 平均', 'color=orange']],
              [],''],

             [[[t, nei3yuan_hi_count, '外三元价格 上升 个数 ', 'color=red'],
               [t, nei3yuan_eq_count, '外三元价格 不变 个数 ', 'color=gray'],
               [t, nei3yuan_lo_count, '外三元价格 下降 个数 ', 'color=darkgreen']],
              [],''],

             [[[t1, nei3yuan_hi_count_avg1, '外三元价格 上升 个数 ' + str(T1) + 'd avg', 'color=red'],
               [t1, nei3yuan_eq_count_avg1, '外三元价格 不变 个数 ' + str(T1) + 'd avg', 'color=gray'],
               [t1, nei3yuan_lo_count_avg1, '外三元价格 下降 个数 ' + str(T1) + 'd avg', 'color=darkgreen']],
              [],''],

             [[[t2, nei3yuan_hi_count_avg2, '外三元价格 上升 个数 ' + str(T2) + 'd avg', 'color=red'],
               [t2, nei3yuan_eq_count_avg2, '外三元价格 不变 个数 ' + str(T2) + 'd avg', 'color=gray'],
               [t2, nei3yuan_lo_count_avg2, '外三元价格 下降 个数 ' + str(T2) + 'd avg', 'color=darkgreen']],
              [],''],
              ]
    plot_many_figure(datas, max_height=900)



    datas = [       
             [[[t2, nei3yuan_hi_avg2, '内三元价格 上升 价格变化求和 ' + str(T2) + 'd avg', 'color=red'],
               [t2, nei3yuan_eq_avg2, '内三元价格 不变 价格变化求和 ' + str(T2) + 'd avg', 'color=gray'],
               [t2, nei3yuan_lo_avg2, '内三元价格 下降 价格变化求和 ' + str(T2) + 'd avg', 'color=darkgreen']],
              [],'30个省'],

             [[[t1, nei3yuan_hi_avg1, '内三元价格 上升 价格变化求和 ' + str(T1) + 'd avg', 'color=red'],
               [t1, nei3yuan_eq_avg1, '内三元价格 不变 价格变化求和 ' + str(T1) + 'd avg', 'color=gray'],
               [t1, nei3yuan_lo_avg1, '内三元价格 下降 价格变化求和 ' + str(T1) + 'd avg', 'color=darkgreen']],
              [],''],
             
             [[[t, nei3yuan_hi, '内三元价格 上升 价格变化求和 ', 'color=red'],
               [t, nei3yuan_eq, '内三元价格 不变 价格变化求和 ', 'color=gray'],
               [t, nei3yuan_lo, '内三元价格 下降 价格变化求和 ', 'color=darkgreen']],
              [],''],
             
             [[[fut_t, fut_price, '生猪 指数', 'color=black'],
               [t, nei3yuan_avg_price, '内三元价格 平均', 'color=orange']],
              [],''],

             [[[t, nei3yuan_hi_count, '内三元价格 上升 个数 ', 'color=red'],
               [t, nei3yuan_eq_count, '内三元价格 不变 个数 ', 'color=gray'],
               [t, nei3yuan_lo_count, '内三元价格 下降 个数 ', 'color=darkgreen']],
              [],''],

             [[[t1, nei3yuan_hi_count_avg1, '内三元价格 上升 个数 ' + str(T1) + 'd avg', 'color=red'],
               [t1, nei3yuan_eq_count_avg1, '内三元价格 不变 个数 ' + str(T1) + 'd avg', 'color=gray'],
               [t1, nei3yuan_lo_count_avg1, '内三元价格 下降 个数 ' + str(T1) + 'd avg', 'color=darkgreen']],
              [],''],

             [[[t2, nei3yuan_hi_count_avg2, '内三元价格 上升 个数 ' + str(T2) + 'd avg', 'color=red'],
               [t2, nei3yuan_eq_count_avg2, '内三元价格 不变 个数 ' + str(T2) + 'd avg', 'color=gray'],
               [t2, nei3yuan_lo_count_avg2, '内三元价格 下降 个数 ' + str(T2) + 'd avg', 'color=darkgreen']],
              [],''],
              ]
    plot_many_figure(datas, max_height=900)



    datas = [       
             [[[t2, tuza_hi_avg2, '土杂猪价格 上升 价格变化求和 ' + str(T2) + 'd avg', 'color=red'],
               [t2, tuza_eq_avg2, '土杂猪价格 不变 价格变化求和 ' + str(T2) + 'd avg', 'color=gray'],
               [t2, tuza_lo_avg2, '土杂猪价格 下降 价格变化求和 ' + str(T2) + 'd avg', 'color=darkgreen']],
              [],'30个省'],

             [[[t1, tuza_hi_avg1, '土杂猪价格 上升 价格变化求和 ' + str(T1) + 'd avg', 'color=red'],
               [t1, tuza_eq_avg1, '土杂猪价格 不变 价格变化求和 ' + str(T1) + 'd avg', 'color=gray'],
               [t1, tuza_lo_avg1, '土杂猪价格 下降 价格变化求和 ' + str(T1) + 'd avg', 'color=darkgreen']],
              [],''],
             
             [[[t, tuza_hi, '土杂猪价格 上升 价格变化求和 ', 'color=red'],
               [t, tuza_eq, '土杂猪价格 不变 价格变化求和 ', 'color=gray'],
               [t, tuza_lo, '土杂猪价格 下降 价格变化求和 ', 'color=darkgreen']],
              [],''],
             
             [[[fut_t, fut_price, '生猪 指数', 'color=black'],
               [t, tuza_avg_price, '土杂猪价格 平均', 'color=orange']],
              [],''],

             [[[t, tuza_hi_count, '土杂猪价格 上升 个数 ', 'color=red'],
               [t, tuza_eq_count, '土杂猪价格 不变 个数 ', 'color=gray'],
               [t, tuza_lo_count, '土杂猪价格 下降 个数 ', 'color=darkgreen']],
              [],''],

             [[[t1, tuza_hi_count_avg1, '土杂猪价格 上升 个数 ' + str(T1) + 'd avg', 'color=red'],
               [t1, tuza_eq_count_avg1, '土杂猪价格 不变 个数 ' + str(T1) + 'd avg', 'color=gray'],
               [t1, tuza_lo_count_avg1, '土杂猪价格 下降 个数 ' + str(T1) + 'd avg', 'color=darkgreen']],
              [],''],

             [[[t2, tuza_hi_count_avg2, '土杂猪价格 上升 个数 ' + str(T2) + 'd avg', 'color=red'],
               [t2, tuza_eq_count_avg2, '土杂猪价格 不变 个数 ' + str(T2) + 'd avg', 'color=gray'],
               [t2, tuza_lo_count_avg2, '土杂猪价格 下降 个数 ' + str(T2) + 'd avg', 'color=darkgreen']],
              [],''],
              ]
    plot_many_figure(datas, max_height=900)


def update_pork_price():
    t = datetime.datetime.now()
    t_str = t.strftime('%Y-%m-%d')

    temp_df = ak.futures_hog_rank(symbol="外三元")
    data = temp_df['价格-公斤'].tolist()
    data = [t_str] + data
    province = temp_df['省份'].tolist()
    for i in range(len(province)):
        province[i] = province[i].strip('市')
        province[i] = province[i].strip('省')
        province[i] = '价格:外三元生猪:'+province[i]
    df = pd.DataFrame(columns=['time']+province, data=[data])
    df.drop(['价格:外三元生猪:西藏'], axis=1, inplace=True)

    temp_df = ak.futures_hog_rank(symbol="内三元")
    data = temp_df['价格-公斤'].tolist()
    data = [t_str] + data
    province = temp_df['省份'].tolist()
    for i in range(len(province)):
        province[i] = province[i].strip('市')
        province[i] = province[i].strip('省')
        province[i] = '价格:内三元生猪:'+province[i]
    df1 = pd.DataFrame(columns=['time']+province, data=[data])
    df1.drop(['价格:内三元生猪:西藏'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    temp_df = ak.futures_hog_rank(symbol="土杂猪")
    data = temp_df['价格-公斤'].tolist()
    data = [t_str] + data
    province = temp_df['省份'].tolist()
    for i in range(len(province)):
        province[i] = province[i].strip('市')
        province[i] = province[i].strip('省')
        province[i] = '价格:土杂猪生猪:'+province[i]
    df1 = pd.DataFrame(columns=['time']+province, data=[data])
    df1.drop(['价格:土杂猪生猪:西藏'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    print(df)

    path = os.path.join(data_dir, '猪价格'+'.csv') 
    old_df = pd.read_csv(path)
    old_df = pd.concat([old_df, df], axis=0)
    cols = old_df.columns.to_list()
    cols.pop(0)
    old_df.drop_duplicates(subset=cols, keep='first', inplace=True)
    old_df.to_csv(path, index=False)



if __name__=="__main__":
    # update_moa_pork_data()
    # test1()

    update_pork_price()
    # test2()

    pass

