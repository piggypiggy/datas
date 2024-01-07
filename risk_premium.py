import os
import time
import numpy as np
import pandas as pd
import datetime
from utils import *
from cftc import *
from chinamoney import *


start_time = '2000-1-1'
end_time = '2025-12-31'

def plot_risk_premium(index_t, index, index_rate, name1, bond_rate_t, bond_rate, name2, T):
    t1, data1 = get_period_data(bond_rate_t, bond_rate, start_time, end_time, remove_nan=True)
    t2, risk_premium = data_sub(index_t, 100/index_rate, t1, data1)

    plot_mean_std([[t2, risk_premium, '风险溢价 = 1/'+name1+' - '+name2]], [[index_t, index, '指数']], T)
    time.sleep(0.5)


def test1():
    path = os.path.join(data_dir, '股指'+'.csv') 
    df = pd.read_csv(path)

    t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    sz50 = np.array(df['上证50_x'], dtype=float)
    cs300 = np.array(df['沪深300_x'], dtype=float)
    zz500 = np.array(df['中证500_x'], dtype=float)
    sz50_pe_ttm = np.array(df['上证50滚动市盈率'], dtype=float)
    cs300_pe_ttm = np.array(df['沪深300滚动市盈率'], dtype=float)
    zz500_pe_ttm = np.array(df['中证500滚动市盈率'], dtype=float)
    sz50_pe_ttm_med = np.array(df['上证50滚动市盈率中位数'], dtype=float)
    cs300_pe_ttm_med = np.array(df['沪深300滚动市盈率中位数'], dtype=float)
    zz500_pe_ttm_med = np.array(df['中证500滚动市盈率中位数'], dtype=float)
    sz50_pb = np.array(df['上证50市净率'], dtype=float)
    cs300_pb = np.array(df['沪深300市净率'], dtype=float)
    zz500_pb = np.array(df['中证500市净率'], dtype=float)  

    path = os.path.join(interest_rate_dir, '国债收益率'+'.csv') 
    df = pd.read_csv(path)

    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cn10y = np.array(df['10Y'], dtype=float)
    cn05y = np.array(df['5Y'], dtype=float)
    rate = cn10y
    rate_name = '中国国债收益率10年'
    # t1, cn10y = get_period_data(t1, cn10y, start_time, end_time, remove_nan=True)

    plot_mean_std([[t0, sz50_pe_ttm, '上证50滚动市盈率']], [], T=250*5, start_time='2012-01-01')
    time.sleep(0.5)
    plot_mean_std([[t0, cs300_pe_ttm, '沪深300滚动市盈率']], [], T=250*5, start_time='2012-01-01')
    time.sleep(0.5)
    plot_mean_std([[t0, zz500_pe_ttm, '中证500滚动市盈率']], [], T=250*5, start_time='2012-01-01')
    time.sleep(0.5)

    plot_risk_premium(t0, sz50, sz50_pe_ttm, '上证50滚动市盈率', t1, rate, rate_name, T=250*5)
    plot_risk_premium(t0, cs300, cs300_pe_ttm, '沪深300滚动市盈率', t1, rate, rate_name, T=250*5)
    plot_risk_premium(t0, zz500, zz500_pe_ttm, '中证500滚动市盈率', t1, rate, rate_name, T=250*5)

    plot_risk_premium(t0, sz50, sz50_pe_ttm_med, '上证50滚动市盈率中位数', t1, rate, rate_name, T=250*5)
    plot_risk_premium(t0, cs300, cs300_pe_ttm_med, '沪深300滚动市盈率中位数', t1, rate, rate_name, T=250*5)
    plot_risk_premium(t0, zz500, zz500_pe_ttm_med, '中证500滚动市盈率中位数', t1, rate, rate_name, T=250*5)

    plot_risk_premium(t0, sz50, sz50_pb, '上证50市净率', t1, rate, rate_name, T=250*5)
    plot_risk_premium(t0, cs300, cs300_pb, '沪深300市净率', t1, rate, rate_name, T=250*5)
    plot_risk_premium(t0, zz500, zz500_pb, '中证500市净率', t1, rate, rate_name, T=250*5)




# 持仓
def test3():
    path = os.path.join(data_dir, '股指'+'.csv') 
    df = pd.read_csv(path).dropna()

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cs300 = np.array(df['沪深300_x'], dtype=float)
    t0, cs300 = get_period_data(t, cs300, start_time, end_time, remove_nan=True)

    cftc_plot_financial(t0, cs300, '沪深300指数', code='244042', inst_name='ICE:MSCI新兴市场指数')


# 地产美元债
def test4():
    path = os.path.join(data_dir, '股指'+'.csv') 
    df = pd.read_csv(path).dropna()

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cs300 = np.array(df['沪深300_x'], dtype=float)
    sz50 = np.array(df['上证50_x'], dtype=float)
    zz500 = np.array(df['中证500_x'], dtype=float)
    t0, cs300 = get_period_data(t, cs300, start_time, end_time, remove_nan=True)
    t1, sz50 = get_period_data(t, sz50, start_time, end_time, remove_nan=True)
    t2, zz500 = get_period_data(t, zz500, start_time, end_time, remove_nan=True)
    
    path = os.path.join(data_dir, '中国房地产美元债 ETF'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    etf = np.array(df['close'], dtype=float)

    c0 = correlation(t0, cs300, t, etf)
    c1 = correlation(t1, sz50, t, etf)
    c2 = correlation(t2, zz500, t, etf)
    plot_two_axis([t0,t1,t2], [cs300,sz50,zz500], ['沪深300指数','上证50指数','中证500指数'], \
                  [t,t,t], [etf,etf,etf], ['中国房地产美元债 ETF','中国房地产美元债 ETF','中国房地产美元债 ETF'], \
                    ['相关系数='+str(c0),'相关系数='+str(c1),'相关系数='+str(c2)], '2021-1-1', end_time)


# 申万行业指数
def test5():
    path = os.path.join(data_dir, '风险溢价'+'.csv') 
    df = pd.read_csv(path).dropna()

    t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    name = df.columns.tolist()
    temp = np.array(name, dtype=str)

    path = os.path.join(data_dir, '中国房地产美元债 ETF'+'.csv') 
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    etf = np.array(df1['close'], dtype=float)

    class1_dict = dict()
    for i in range(len(temp)):
        if ('申万一级指数(旧)' in temp[i]):
            data = np.array(df[temp[i]], dtype=float)
            c0 = correlation(t0, data, t1, etf)
            class1_dict[temp[i]] = c0
    # 排序
    class1_dict_sorted = sorted(class1_dict.items(),key=lambda s:s[1])
    print(class1_dict_sorted)

    class2_dict = dict()
    for i in range(len(temp)):
        if ('申万二级指数(旧)' in temp[i]):
            data = np.array(df[temp[i]], dtype=float)
            c0 = correlation(t0, data, t1, etf)
            class2_dict[temp[i]] = c0
    # 排序
    class2_dict_sorted = sorted(class2_dict.items(),key=lambda s:s[1])
    print(class2_dict_sorted)


    t0_list = list()
    data0_list = list()
    name0_list = list()
    t1_list = list()
    data1_list = list()
    name1_list = list()
    title_list = list()
    for i in range(5):
        t0_list.append(t0)
        data0_list.append(np.array(df[class1_dict_sorted[i][0]], dtype=float))
        name0_list.append(class1_dict_sorted[i][0])
        t1_list.append(t1)
        data1_list.append(etf)
        name1_list.append('中国房地产美元债 ETF')
        title_list.append('相关系数='+str(class1_dict_sorted[i][1]))

    plot_two_axis(t0_list, data0_list, name0_list, t1_list, data1_list, name1_list, title_list, '2021-1-1', end_time)


    t0_list = list()
    data0_list = list()
    name0_list = list()
    t1_list = list()
    data1_list = list()
    name1_list = list()
    title_list = list()
    for i in range(5):
        t0_list.append(t0)
        data0_list.append(np.array(df[class1_dict_sorted[-i-1][0]], dtype=float))
        name0_list.append(class1_dict_sorted[-i-1][0])
        t1_list.append(t1)
        data1_list.append(etf)
        name1_list.append('中国房地产美元债 ETF')
        title_list.append('相关系数='+str(class1_dict_sorted[-i-1][1]))

    plot_two_axis(t0_list, data0_list, name0_list, t1_list, data1_list, name1_list, title_list, '2021-1-1', end_time)


    t0_list = list()
    data0_list = list()
    name0_list = list()
    t1_list = list()
    data1_list = list()
    name1_list = list()
    title_list = list()
    for i in range(5):
        t0_list.append(t0)
        data0_list.append(np.array(df[class2_dict_sorted[i][0]], dtype=float))
        name0_list.append(class2_dict_sorted[i][0])
        t1_list.append(t1)
        data1_list.append(etf)
        name1_list.append('中国房地产美元债 ETF')
        title_list.append('相关系数='+str(class2_dict_sorted[i][1]))

    plot_two_axis(t0_list, data0_list, name0_list, t1_list, data1_list, name1_list, title_list, '2021-1-1', end_time)


    t0_list = list()
    data0_list = list()
    name0_list = list()
    t1_list = list()
    data1_list = list()
    name1_list = list()
    title_list = list()
    for i in range(5):
        t0_list.append(t0)
        data0_list.append(np.array(df[class2_dict_sorted[-i-1][0]], dtype=float))
        name0_list.append(class2_dict_sorted[-i-1][0])
        t1_list.append(t1)
        data1_list.append(etf)
        name1_list.append('中国房地产美元债 ETF')
        title_list.append('相关系数='+str(class2_dict_sorted[-i-1][1]))

    plot_two_axis(t0_list, data0_list, name0_list, t1_list, data1_list, name1_list, title_list, '2021-1-1', end_time)


# 大盘拥挤度 https://legulegu.com/stockdata/ashares-congestion
def test6():
    path = os.path.join(data_dir, '股指'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    sz50 = np.array(df['上证50_x'], dtype=float)
    cs300 = np.array(df['沪深300_x'], dtype=float)
    zz500 = np.array(df['中证500_x'], dtype=float)
    congestion = np.array(df['congestion'], dtype=float)

    plot_two_axis([t,t,t],[sz50,cs300,zz500],['上证50','沪深300','中证500'],
                  [t,t,t],[congestion,congestion,congestion],['大盘拥挤度','大盘拥挤度','大盘拥挤度'],
                  ['','',''],start_time,end_time)


# 股指 和 中美利差
def plot_cs300_vs_rate():
    path = os.path.join(data_dir, '股指'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cs300 = np.array(df['沪深300_x'], dtype=float)
    sz50 = np.array(df['上证50_x'], dtype=float)

    path = os.path.join(interest_rate_dir, '国债收益率'+'.csv') 
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))

    path = os.path.join(interest_rate_dir, 'us_yield_curve'+'.csv') 
    df2 = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))

    us10y = np.array(df2['10Y'], dtype=float)
    cn10y = np.array(df1['10Y'], dtype=float)
    us02y = np.array(df2['2Y'], dtype=float)
    cn02y = np.array(df1['2Y'], dtype=float)
    t3, diff2 = data_sub(t1, cn10y, t2, us10y)
    t4, diff3 = data_sub(t1, cn02y, t2, us02y)

    datas = [
             [[[t,cs300,'沪深300','color=black'],],
              [[t3,diff2,'中美利差 10Y','color=orange'],
               [t4,diff3,'中美利差 2Y','color=blue'],],''],]
    
    plot_many_figure(datas, start_time='2017-01-01')

    datas = [
             [[[t,sz50,'上证50','color=black'],],
              [[t3,diff2,'中美利差 10Y','color=orange'],
               [t4,diff3,'中美利差 2Y','color=blue'],],''],]
    
    plot_many_figure(datas, start_time='2017-01-01')


# 股指 和 房价
def plot_cs300_vs_house_price():
    path = os.path.join(data_dir, '股指'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cs300 = np.array(df['沪深300_x'], dtype=float)
    sz50 = np.array(df['上证50_x'], dtype=float)

    path = os.path.join(nbs_dir, '70个大中城市住宅销售价格指数'+'.csv') 
    df = pd.read_csv(path, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m'))

    # 上涨、持平、下跌城市个数
    mom_new_hi_count, mom_new_eq_count, mom_new_lo_count = get_cs_price_change_count(df, '新建商品住宅销售价格指数(上月=100)')
    yoy_new_hi_count, yoy_new_eq_count, yoy_new_lo_count = get_cs_price_change_count(df, '新建商品住宅销售价格指数(上年同月=100)')
    mom_seh_hi_count, mom_seh_eq_count, mom_seh_lo_count = get_cs_price_change_count(df, '二手住宅销售价格指数(上月=100)')
    yoy_seh_hi_count, yoy_seh_eq_count, yoy_seh_lo_count = get_cs_price_change_count(df, '二手住宅销售价格指数(上年同月=100)')

    datas = [
             [[[t,cs300,'沪深300','color=black'],],
              [[t1,mom_new_lo_count,'新建商品住宅销售价格 下跌城市个数','color=darkgreen'],
               [t1,mom_new_hi_count,'新建商品住宅销售价格 上涨城市个数','color=red'],
               ],''],]

    plot_many_figure(datas, start_time='2012-01-01')


if __name__=="__main__":
    # # 股指 和 房价
    plot_cs300_vs_house_price()

    # 股指 和 中美利差
    plot_cs300_vs_rate()

    # 风险溢价
    test1()

    # # 融资融券
    # test2()

    # cs300, MSCI EM 持仓
    test3()

    # 股指 和 地产美元债ETF
    test4()

    ## 申万行业 和 地产美元债ETF
    # test5()

    # 大盘拥挤度 https://legulegu.com/stockdata/ashares-congestion
    test6()

    # # RMBS条件早偿率指数
    # plot_rmbs()

    # # CDS
    plot_china_cds()
