#利润表=lrb，资产负债表=zcfzb，现金流量表=xjllb
#参考 https://zhuanlan.zhihu.com/p/44753154


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
from bokeh.models import LinearAxis, Range1d, VBar
from scipy.stats import linregress
from utils import *
import requests, urllib


cur_dir = os.path.dirname(__file__)
data_dir = os.path.join(cur_dir, 'data')


def transpose_csv(path):
    df = pd.read_csv(path, header=None, encoding='utf-8')
    data = df.values

    # 删除最后一列的逗号
    data = data[:, :-1]
    # 把列的顺序反过来
    data[:, 1:] = data[:, -1:0:-1]

    # 转置
    index1 = list(df.keys())[:-1]
    data = list(map(list, zip(*data)))
    data = pd.DataFrame(data, index = index1)

    data.to_csv(path, header=None, index=None)

def get_table(url, path):
    while True:
        try:
            content = urllib.request.urlopen(url, timeout=2).read()
            content = content.decode("gbk")
            content = content.replace('--', '')
            content = content.replace(' ', '')
            # content = content.replace(' 报告日期', 'time')
            content = content.replace('报告日期', 'time')
            content = content.replace('(万元,', '(万元),')
            content = content.encode("utf8")

            with open(path, 'wb') as f:
                f.write(content)
                f.close()

            transpose_csv(path)
            break
            # sleep(1)

        except urllib.error.HTTPError as e:
            print(e)
            continue

        # except urllib.error.URLError as e:
        #     print(e)

        except Exception as e:
            print(e)
            break

def get_tables(stock):
    lrb_url = 'http://quotes.money.163.com/service/lrb_' + str(stock) + '.html'
    zcfzb_url = 'http://quotes.money.163.com/service/zcfzb_' + str(stock) + '.html'
    xjllb_url = 'http://quotes.money.163.com/service/xjllb_' + str(stock) + '.html'

    stock_data_dir = os.path.join(data_dir, 'stock', stock)
    if not os.path.exists(stock_data_dir):
        os.mkdir(stock_data_dir)

    print(stock_data_dir)
    # 利润表
    print('get income statement...')
    get_table(lrb_url, os.path.join(stock_data_dir, 'income statement.csv'))
    # 资产负债表
    print('get balance sheet...')
    get_table(zcfzb_url, os.path.join(stock_data_dir, 'balance sheet.csv'))
    # 资金流量表
    print('get cash flow statement...')
    get_table(xjllb_url, os.path.join(stock_data_dir, 'cash flow statement.csv'))

    # path = data_dir + 'income statement.csv'
    # df = pd.read_csv(path)
    # path = data_dir + 'balance sheet.csv'
    # df = pd.read_csv(path)
    # path = data_dir + 'cash flow statement.csv'
    # df = pd.read_csv(path)
    # print(df['销售商品、提供劳务收到的现金(万元)'])


def get_all_stock_tables():
    stock_list_path = os.path.join(cur_dir, "stock_list.csv")
    df = pd.read_csv(stock_list_path)
    code = np.array(df['代码'])
    name = np.array(df['名称'])

    L = len(df)
    for i in range(L):
        # print(code[i][2:])
        print(code[i][2:])
        if(code[i][0:2] != 'bj'):
            get_tables(code[i][2:])
            
    pass

# def get_stock_category(code):
#     df = ak.stock_industry_change_cninfo(symbol='688288', start_date="20161227", end_date="20220708")
#     a = df.query('分类标准 == ["中证行业分类标准"]')
#     print(a)

# 获取每个上市公司所处的行业(中证行业分类标准), 烂网站, 没拿完数据就挂了
def get_all_stock_category():

    stock_list_path = os.path.join(cur_dir, "stock_list.csv")
    df = pd.read_csv(stock_list_path)
    df = pd.read_csv(stock_list_path)
    code = np.array(df['代码'])
    name = np.array(df['名称'])

    lst = ['code', 'name', '一级行业', '二级行业', '三级行业', '四级行业']
    df1 = pd.DataFrame(columns = lst)

    L = len(df)
    k = 0
    for i in range(600,601):
        # print(code[i][2:])
        print(code[i][2:])
        # try:
        df = ak.stock_industry_change_cninfo(symbol=code[i][2:], start_date="20191227", end_date="20220901")
        a = df.query('分类标准 == ["中证行业分类标准"]')
        print(a)
        df1.loc[k] = [code[i][2:], name[i], np.array(a['行业门类'])[-1], np.array(a['行业次类'])[-1], np.array(a['行业大类'])[-1], np.array(a['行业中类'])[-1]]
        k = k + 1
        # except Exception as e:
        #     print(e)
        #     continue

    stock_category_path = os.path.join(cur_dir, "stock_category.csv")
    df1.to_csv(stock_category_path, index=None)

# 统计上市企业财务报表
def stock_financial_statistics():
    path = os.path.join(cur_dir, "中证行业分类结果.csv")
    df = pd.read_csv(path, dtype='str')

    level1 = np.array(df['新中证一级简称'])
    level2 = np.array(df['新中证二级简称'])
    level3 = np.array(df['新中证三级简称'])
    level4 = np.array(df['新中证四级简称'])

    idx = np.where(level4 == '焦炭')
    # print(df.iloc[idx]['code'])
    codes = df.iloc[idx]['code']

    for i in range(len(idx)):
        # 股票财务表的目录
        print(codes.iloc[i])
        stock_data_dir = os.path.join('D:\上市公司财报\stock\\', codes.iloc[i])
        balance_sheet_path = os.path.join(stock_data_dir, 'income statement.csv')
        # 表中没有的数据设成0
        balance_sheet_df = pd.read_csv(balance_sheet_path).fillna('0')
        # balance_sheet_df[balance_sheet_df == nan] = '0'
        # print(balance_sheet_df == np.nan)
        print(balance_sheet_df.iloc[2,2])
        t0 = pd.DatetimeIndex(pd.to_datetime(balance_sheet_df['time'], format='%Y-%m-%d'))
        income0 = np.array(balance_sheet_df['营业总收入(万元)'], dtype=float)
        cost0 = np.array(balance_sheet_df['营业总成本(万元)'], dtype=float)
        profit0 = income0 - cost0

        if i == 0:
            t = t0.copy()
            profit = profit0.copy()
        else:
            t, profit = data_add(t, profit, t0, profit0)

    plot_seasonality(t, profit, start_year=2014, title='上市公司利润 普钢(万元)')

stock_financial_statistics()
# get_all_stock_category()
# get_all_table('430047')
# get_all_stock_tables()
