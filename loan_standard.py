import os
import time
import math
import numpy as np
import pandas as pd
import datetime
from utils import *


# 收紧放贷条件 银行贷款数量
def update_loan_standard():
    code = [ 
          ['DRTSCLCC', 'Net Percentage of Domestic Banks Tightening Standards for Credit Card Loans'],
          ['DRTSCILM', 'Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Large and Middle-Market Firms'],
          ['DRTSCIS', 'Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Small Firms'],
          ['TOTCI', 'Commercial and Industrial Loans, All Commercial Banks'],
          ['TOTCINSA', 'Commercial and Industrial Loans, All Commercial Banks NSA'],
          ['CCLACBW027SBOG', 'Consumer Loans: Credit Cards and Other Revolving Plans, All Commercial Banks'],
          ['CCLACBW027NBOG', 'Consumer Loans: Credit Cards and Other Revolving Plans, All Commercial Banks NSA'],
          ['CLSACBW027SBOG', 'Consumer Loans, All Commercial Banks'],
          ['CLSACBW027NBOG', 'Consumer Loans, All Commercial Banks NSA'],
        ]

    name_code = {'loan_standard': code}
    update_fred_data(name_code, data_dir)


def test1():
    path = os.path.join(data_dir, 'loan_standard'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    standard1 = np.array(df['Net Percentage of Domestic Banks Tightening Standards for Credit Card Loans'], dtype=float)
    standard2 = np.array(df['Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Large and Middle-Market Firms'], dtype=float)
    standard3 = np.array(df['Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Small Firms'], dtype=float)

    path = os.path.join(interest_rate_dir, 'us_yield_spread'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    avg = np.array(df['US Corporate Spread'], dtype=float)
    aaa = np.array(df['AAA US Corporate Spread'], dtype=float)
    aa = np.array(df['AA US Corporate Spread'], dtype=float)
    a = np.array(df['A US Corporate Spread'], dtype=float)
    bbb = np.array(df['BBB US Corporate Spread'], dtype=float)
    bb = np.array(df['BB US Corporate Spread'], dtype=float)
    b = np.array(df['B US Corporate Spread'], dtype=float)
    ccc = np.array(df['CCC AND LOWER US Corporate Spread'], dtype=float)
    em = np.array(df['Emerging Markets Corporate Spread'], dtype=float)
    moody_spread = np.array(df['Moody Seasoned Aaa Corporate Bond Minus Federal Funds Rate'], dtype=float)

    datas = [[[[t1,standard1,'Net Percentage Tightening Standards for Credit Card Loans',''],
               [t1,standard2,'Net Percentage Tightening Standards for Commercial and Industrial Loans to Large and Middle-Market Firms',''],
               [t1,standard3,'Net Percentage Tightening Standards for Commercial and Industrial Loans to Small Firms',''],
               ],[],''],

               [[[t2,avg,'US Corporate Spread',''],
                [t2,aaa,'AAA US Corporate Spread',''],
                [t2,aa,'AA US Corporate Spread',''],
                [t2,a,'A US Corporate Spread',''],],[],''],

              [[[t2,bbb,'BBB US Corporate Spread',''],
                [t2,bb,'BB US Corporate Spread',''],
                [t2,b,'B US Corporate Spread',''],
                [t2,ccc,'CCC US Corporate Spread',''],
                [t2,em,'Emerging Markets Corporate Spread','']],[],'']]

    start_time = '1995-01-01'
    end_time = '2100-01-01'
    plot_many_figure(datas, start_time=start_time, end_time=end_time)


# 收紧放贷条件的银行占比 和 银行贷款数量增速
def test2():
    path = os.path.join(data_dir, 'loan_standard'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    standard1 = np.array(df['Net Percentage of Domestic Banks Tightening Standards for Credit Card Loans'], dtype=float)
    standard2 = np.array(df['Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Large and Middle-Market Firms'], dtype=float)
    standard3 = np.array(df['Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Small Firms'], dtype=float)

    loans = np.array(df['Consumer Loans, All Commercial Banks'], dtype=float)
    loans1 = np.array(df['Consumer Loans: Credit Cards and Other Revolving Plans, All Commercial Banks'], dtype=float)
    loans2 = np.array(df['Commercial and Industrial Loans, All Commercial Banks'], dtype=float)

    t_, loans = get_period_data(t, loans, start='1995-01-01', remove_nan=True)
    t1, loans1 = get_period_data(t, loans1, start='1995-01-01', remove_nan=True)
    t2, loans2 = get_period_data(t, loans2, start='1995-01-01', remove_nan=True)

    t_, loans_yoy = yoy(t_, loans)
    t1, loans1_yoy = yoy(t1, loans1)
    t2, loans2_yoy = yoy(t2, loans2)

    datas = [[[[t2,loans2_yoy,'Commercial and Industrial Loans YOY','']],
              [[t+pd.Timedelta(days=365),standard2,'收紧放贷条件的银行占比(对大中企业) 右移12M',''],
               [t+pd.Timedelta(days=365),standard3,'收紧放贷条件的银行占比(对小型企业) 右移12M','']],''],]
    plot_many_figure(datas)

    time.sleep(0.5)
    datas = [[[[t1,loans1_yoy,'Consumer Loans: Credit Cards and Other Revolving Plans YOY','']],
              [[t+pd.Timedelta(days=365),standard2,'收紧放贷条件的银行占比(对大中企业) 右移12M',''],
               [t+pd.Timedelta(days=365),standard3,'收紧放贷条件的银行占比(对小型企业) 右移12M','']],''],]
    plot_many_figure(datas)

    time.sleep(0.5)
    path = os.path.join(data_dir, '中美PMI'+'.csv') 
    df = pd.read_csv(path)
    t3 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    pmi = np.array(df['美国:供应管理协会(ISM):PMI:季调'], dtype=float)
    pmi1 = np.array(df['美国:ISM:PMI:新订单:季调'], dtype=float)

    datas = [[[[t2,loans2_yoy,'Commercial and Industrial Loans YOY','']],
              [[t3+pd.Timedelta(days=30*15),pmi,'美国:供应管理协会(ISM):PMI:季调 右移15M',''],
               [t3+pd.Timedelta(days=30*15),pmi1,'美国:ISM:PMI:新订单:季调 右移15M','']],''],]
    plot_many_figure(datas)


if __name__=="__main__":
    update_loan_standard()

    # us yield spread
    test1()

    # 收紧放贷条件的银行占比 和 银行贷款数量增速
    test2()




