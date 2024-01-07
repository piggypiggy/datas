import os
import numpy as np
import pandas as pd
import datetime
from scipy.stats import linregress
from code import *
from utils import *



fut_list = [
    # CFFEX
    ['cffex', 'IF'],
    ['cffex', 'IH'],
    ['cffex', 'IC'],
    ['cffex', 'TF'],
    ['cffex', 'T'],
    ['cffex', 'TS'],
    ['cffex', 'IM'],
    # SHFE
    ['shfe', 'au'],
    ['shfe', 'ag'],
    ['shfe', 'cu'],
    ['shfe', 'al'],
    ['shfe', 'pb'],
    ['shfe', 'zn'],
    ['shfe', 'ni'],
    ['shfe', 'sn'],
    ['shfe', 'ss'],
    ['shfe', 'ru'],
    ['shfe', 'rb'],
    ['shfe', 'hc'],
    ['shfe', 'fu'],
    ['shfe', 'bu'],
    ['shfe', 'ao'],
    # INE
    ['shfe', 'sc'],
    ['shfe', 'nr'],
    ['shfe', 'lu'],
    ['shfe', 'bc'],
    # dce
    ['dce', 'a'],
    ['dce', 'b'],
    ['dce', 'c'],
    ['dce', 'cs'],
    ['dce', 'i'],
    ['dce', 'j'],
    ['dce', 'jm'],
    ['dce', 'jd'],
    ['dce', 'l'],
    ['dce', 'm'],
    ['dce', 'p'],
    ['dce', 'pp'],
    ['dce', 'v'],
    ['dce', 'y'],
    ['dce', 'eg'],
    ['dce', 'eb'],
    ['dce', 'pg'],
    ['dce', 'lh'],
    # CZCE
    ['czce', 'CF'],
    ['czce', 'CJ'],
    ['czce', 'SR'],
    ['czce', 'TA'],
    ['czce', 'OI'],
    ['czce', 'MA'],
    ['czce', 'FG'],
    ['czce', 'SF'],
    ['czce', 'SM'],
    ['czce', 'AP'],
    ['czce', 'PK'],
    ['czce', 'UR'],
    ['czce', 'SA'],
    ['czce', 'PF'],
    # GFEX
    ['gfex', 'si'],
]

def calculate_all_future_correlation():
    L = len(fut_list)
    # L=4
    price_list = []
    for i in range(L):
        path = os.path.join(future_price_dir, fut_list[i][0], fut_list[i][1]+'.csv')
        df = pd.read_csv(path, header=[0,1])
        t = pd.DatetimeIndex(pd.to_datetime(df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
        price = np.array(df['c2']['close'], dtype=float)
        w = np.where(price > 1)[0]
        t = t[w]
        price = price[w]
        t, price = get_period_data(t, price, '2021-01-01', '2099-01-01')
        price_list.append([fut_list[i][0] + ' ' + fut_list[i][1], t, price])

    n = 250
    rs_list = []
    for i in range(L):
        for k in range(i+1, L):
            t1 = price_list[i][1]
            price1 = price_list[i][2]
            t2 = price_list[k][1]
            price2 = price_list[k][2]

            idx1 = np.isin(t1, t2)
            idx2 = np.isin(t2, t1)
            t1 = t1[idx1]
            price1 = price1[idx1]
            t2 = t2[idx2]
            price2 = price2[idx2]

            try:
                _, _, r1, _, _ = linregress(price1[-n:], price2[-n:])
            except:
                r1 = 0
            try:
                _, _, r2, _, _ = linregress(price1[-n*2:], price2[-n*2:])
            except:
                r2 = 0

            rs_list.append([price_list[i][0] + ', ' + price_list[k][0], r1*r1, r2*r2])

    rs = np.array(rs_list)
    y1 = rs[:,1].astype(float)
    order = np.argsort(y1)
    print('=================1Y TOP 100=================')
    print(rs[order[-100:],:])

    y2 = rs[:,2].astype(float)
    order = np.argsort(y2)
    print('=================2Y TOP 100=================')
    print(rs[order[-100:],:])



def xxx():
    tb, _, _, _, b = get_future_inst_id_data('dce', 'b2112')
    tm, _, _, _, m = get_future_inst_id_data('dce', 'm2201')
    ty, _, _, _, y = get_future_inst_id_data('dce', 'y2201')

    t, tmp = data_add(ty, y*1, tm, m*4)
    t2, b_profit = data_sub(t, tmp, tb, b*5)


    datas = [
             [[[tb,b,'b',''],],
              [[t2,b_profit,'b profit',''],],''],
    ]
    plot_many_figure(datas)

xxx()
# calculate_all_future_correlation()
# plot_future_correlation('dce', 'eb', 'dce', 'pp')
# plot_future_correlation('dce', 'v', 'dce', 'eg')
# plot_future_correlation('dce', 'pg', 'dce', 'eg')
# plot_future_correlation('dce', 'c', 'dce', 'cs')
# plot_future_correlation('dce', 'l', 'dce', 'v')
# plot_future_correlation('shfe', 'sc', 'czce', 'TA')
# plot_future_correlation('shfe', 'au', 'shfe', 'ag')

# plot_future_correlation('shfe', 'cu', 'shfe', 'al')