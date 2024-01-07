import os
import threading
import time
import math
import numpy as np
import akshare as ak
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, NumeralTickFormatter
from scipy.stats import linregress
from utils import *

cur_dir = os.path.dirname(__file__)
data_dir = os.path.join(cur_dir, 'data')
stock_dir = 'D:\\finance\A\stock'

# 企(事)业单位贷款:中长期贷款同比 和 股指
def test1():
    start_time = '2012-1-1'
    end_time = '2022-12-31'

    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)

    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    shfe_cu = np.array(df1['期货收盘价(主力):阴极铜'], dtype=float)

    t1, shfe_cu = get_period_data(t1, shfe_cu, '2020-01-01', '2120-01-01', remove_nan=True)

    # 读取 stock_list.csv
    list_path = os.path.join(stock_dir, "stock_list.csv")
    with open(list_path, encoding = 'utf_8_sig') as csvfile:
        reader = csv.reader(csvfile)
        # 所有股票的代码
        stock_zh_a_symbol_list = [row[0] for row in reader]
        # 去掉csv第一行的中文
        stock_zh_a_symbol_list.pop(0)

        L = len(stock_zh_a_symbol_list)
        # L=100
        # stock_zh_a_symbol_list = stock_zh_a_symbol_list[:L]
        cor = np.zeros((L), dtype=float)
        for i in range(L):
            # 日线数据存储在 symbol.csv
            data_path = os.path.join(stock_dir, stock_zh_a_symbol_list[i] + ".csv")
            df2 = pd.read_csv(data_path)
            t2 = pd.DatetimeIndex(pd.to_datetime(df2['date'], format='%Y-%m-%d'))
            stock_price = np.array(df2['close'], dtype=float)

            cor[i] = correlation(t1, shfe_cu, t2, stock_price)

    order = np.argsort(cor)[::-1]
    cor_order = [cor[i] for i in order]
    stock_zh_a_symbol_list_order = [stock_zh_a_symbol_list[i] for i in order]


    for i in range(50):
        print(stock_zh_a_symbol_list_order[i], cor_order[i])

    print('--------------------------------------')
    for i in range(50):
        print(stock_zh_a_symbol_list_order[L-i-1], cor_order[L-i-1])

test1()
