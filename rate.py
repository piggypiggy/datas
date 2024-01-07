import os
import time
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
from fredapi import Fred
from chinamoney import *
from fx import plot_cny



# 国内的一些利率
def plot_china_rate():
    path = os.path.join(interest_rate_dir, '国债收益率'+'.csv') 
    df = pd.read_csv(path)
    gov_t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cn01y = np.array(df['1Y'], dtype=float)
    cn02y = np.array(df['2Y'], dtype=float)
    cn05y = np.array(df['5Y'], dtype=float)
    cn10y = np.array(df['10Y'], dtype=float)
    cn30y = np.array(df['30Y'], dtype=float)


    path = os.path.join(interest_rate_dir, '同业存单'+'.csv') 
    df = pd.read_csv(path)
    ib_t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    ib03m = np.array(df['AAA:3M'], dtype=float)
    ib01y = np.array(df['AAA:1Y'], dtype=float)


    path = os.path.join(interest_rate_dir, 'shibor'+'.csv') 
    df = pd.read_csv(path)
    shibor_t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    shiboron = np.array(df['ON'], dtype=float)
    shibor3m = np.array(df['3M'], dtype=float)


    path = os.path.join(interest_rate_dir, '回购定盘利率'+'.csv') 
    df = pd.read_csv(path)
    fr_t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    fr007 = np.array(df['FR007'], dtype=float)
    fdr007 = np.array(df['FDR007'], dtype=float)


    path = os.path.join(interest_rate_dir, '地方政府债'+'.csv') 
    df = pd.read_csv(path)
    lgfv_t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lgfv_1Y = np.array(df['AAA-:1Y'], dtype=float)


    path = os.path.join(interest_rate_dir, 'LPR'+'.csv') 
    df = pd.read_csv(path)
    lpr_t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lpr1y = np.array(df['1Y'], dtype=float)
    lpr5y = np.array(df['5Y'], dtype=float)

    # rrp007 = np.array(df['公开市场操作:逆回购:7天:中标利率'], dtype=float)
    # mlf = np.array(df['中期借贷便利(MLF):操作利率:1年'], dtype=float)
    # rr = np.array(df['人民币存款准备金率:大型存款类金融机构(变动日期)'], dtype=float)


    datas = [
             [[[gov_t,cn02y,'国债收益率:2Y',''],
               [gov_t,cn05y,'国债收益率:5Y',''],
               [gov_t,cn10y,'国债收益率:10Y',''],
               [gov_t,cn30y,'国债收益率:30Y','']],[],''],

             [[[ib_t,ib03m,'同业存单AAA:3M',''],
               [ib_t,ib01y,'同业存单AAA:1Y',''],
               [gov_t,cn01y,'国债收益率:1Y',''],
               [fr_t,fr007,'FR007',''],
               [fr_t,fdr007,'FDR007',''],],[],''],

             [[[ib_t,ib03m,'同业存单AAA:3M',''],
               [ib_t,ib01y,'同业存单AAA:1Y',''],
               [shibor_t,shiboron,'SHIBOR:ON',''],
               [shibor_t,shibor3m,'SHIBOR:3M',''],],[],''],

             [[[lpr_t,ib03m,'LPR:1Y',''],
               [lpr_t,ib01y,'LPR:5Y',''],],[],''],
    ]
    plot_many_figure(datas, start_time='2015-01-01')

    t1, diff1 = data_sub(gov_t, cn10y, ib_t, ib01y)
    datas = [
             [[[gov_t,cn10y,'国债收益率:10Y',''],
               [ib_t,ib01y,'同业存单AAA:1Y',''],],[],''],

             [[[t1, diff1,'国债收益率:10Y - 同业存单AAA:1Y','style=vbar']],[],''],
    ]
    plot_many_figure(datas, start_time='2015-01-01')


    t1, diff = data_sub(lgfv_t, lgfv_1Y, gov_t, cn01y)
    datas = [
             [[[t1, diff,'地方政府债AAA-:1Y - CN1Y','color=black'],
               ],
               [],''],

             [[[lgfv_t,lgfv_1Y,'地方政府债AAA-:1Y',''],
                [gov_t,cn01y,'国债收益率:1Y','']
               ],
               [],''],
    ]
    plot_many_figure(datas, start_time='2015-01-01')





# (社融同比 - M2同比) 和 同业存单利率,国债利率
def z():
    start_time = '2010-2-1'
    end_time = '2029-10-10'

    path = os.path.join(data_dir, '利率'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    ib_deposit_rate = np.array(df['同业存单(AAA)收盘到期收益率:1年'], dtype=float)
    cn10y = np.array(df['中国国债收益率10年'], dtype=float)
    t1, ib_deposit_rate = get_period_data(t, ib_deposit_rate, start_time, end_time, remove_nan=True)
    t5, cn10y = get_period_data(t, cn10y, start_time, end_time, remove_nan=True)


    path2 = os.path.join(data_dir, '货币供应量'+'.csv') 
    df2 = pd.read_csv(path2).fillna('0')
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m'))
    m2 = np.array(df2['M2'], dtype=float)
    t2, m2 = get_period_data(t2, m2, start_time, end_time)

    path3 = os.path.join(data_dir, '社会融资规模'+'.csv') 
    df3 = pd.read_csv(path3).fillna('0')
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m'))
    sf = np.array(df3['社会融资规模存量'], dtype=float)   
    t3, sf = get_period_data(t3, sf, start_time, end_time)

    t2_yoy, m2_yoy = yoy(t2, m2)
    t3_yoy, sf_yoy = yoy(t3, sf)
    t_yoy, dff_yoy = data_sub(t3_yoy, sf_yoy, t2_yoy, m2_yoy)

    fig1 = figure(frame_width=1800, frame_height=300, tools=TOOLS, x_axis_type = "datetime")
    fig1.line(t_yoy, dff_yoy, line_width=2, line_color='black', legend_label='社融同比 - M2同比')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    fig2 = figure(frame_width=1800, frame_height=300, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.line(t1, ib_deposit_rate/100, line_width=2, line_color='orange', legend_label='同业存单利率AAA 1年')
    fig2.line(t5, cn10y/100, line_width=2, line_color='blue', legend_label='中国国债收益率:10年')
    fig2.yaxis[0].formatter = NumeralTickFormatter(format='0.0%')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    show(column(fig1,fig2))



# HK
def plot_hk_rate():
    start_time = '2015-1-1'

    path = os.path.join(hkma_dir, 'hibor'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    hibor_on = np.array(df['ir_overnight'], dtype=float)
    hibor_1m = np.array(df['ir_1m'], dtype=float)
    hibor_3m = np.array(df['ir_3m'], dtype=float)
    hibor_1y = np.array(df['ir_12m'], dtype=float)

    path = os.path.join(data_dir, 'libor'+'.csv') 
    df = pd.read_csv(path)
    t3 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    libor_3m = np.array(df['3M'], dtype=float)

    path = os.path.join(interest_rate_dir, 'federal_fund_rate'+'.csv') 
    df = pd.read_csv(path)
    t5 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    effr = np.array(df['Federal Funds Effective Rate'], dtype=float)

    t4, diff_3m = data_sub(t3, libor_3m, t, hibor_3m)
    t6, diff_on = data_sub(t5, effr, t, hibor_on)

    path = os.path.join(hkma_dir, 'USDHKD'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    usdhkd = np.array(df['hkd_fer_spot'], dtype=float)
    
    path = os.path.join(hkma_dir, 'market_operation'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    balance = np.array(df['closing_balance'], dtype=float)

    path = os.path.join(data_dir, '恒生指数'+'.csv') 
    df = pd.read_csv(path)
    t01 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    hsi = np.array(df['close'], dtype=float)

    path = os.path.join(data_dir, '恒生科技指数'+'.csv') 
    df = pd.read_csv(path)
    t02 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    hstect = np.array(df['close'], dtype=float)

    path = os.path.join(data_dir, '恒生中国企业指数'+'.csv') 
    df = pd.read_csv(path)
    t03 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    hscei = np.array(df['close'], dtype=float)

    datas = [
            [[[t,hibor_3m,'hibor ON',''],
              [t5,effr,'EFFR',''],], 
             [[t6,diff_on, 'libor ON - EFFR','style=vbar'],],''],

            [[[t,hibor_3m,'hibor 3M',''],
              [t3,libor_3m,'libor 3M',''],], 
             [[t4,diff_3m, 'libor - hibor','style=vbar'],],''],

            [[[t1,usdhkd,'USDHKD','color=black'],], [],''],

            [[[t02,hstect,'HSTECH','color=black'],], [],''],
            [[[t03,hscei,'HSCEI','color=black'],], [],''],

            [[[t2,balance,'HKMA BALANCE','color=red'],], [],''],
    ]

    plot_many_figure(datas, max_height=1000, start_time=start_time)


# 汇率 和 利差
def plot_fx_vs_rate():
    path = os.path.join(fx_dir, 'USDCNY'+'.csv')
    usdcny_df = pd.read_csv(path)
    usdcny_t = pd.DatetimeIndex(pd.to_datetime(usdcny_df['time'], format='%Y-%m-%d'))
    usdcny = np.array(usdcny_df['close'], dtype=float)

    path = os.path.join(fx_dir, 'USDJPY'+'.csv')
    usdjpy_df = pd.read_csv(path)
    usdjpy_t = pd.DatetimeIndex(pd.to_datetime(usdjpy_df['time'], format='%Y-%m-%d'))
    usdjpy = np.array(usdjpy_df['close'], dtype=float)

    path = os.path.join(interest_rate_dir, 'us_yield_curve'+'.csv')
    us_df = pd.read_csv(path)
    us_t = pd.DatetimeIndex(pd.to_datetime(us_df['time'], format='%Y-%m-%d'))
    us10y = np.array(us_df['10Y'], dtype=float)

    path = os.path.join(interest_rate_dir, '国债收益率'+'.csv')
    cn_df = pd.read_csv(path)
    cn_t = pd.DatetimeIndex(pd.to_datetime(cn_df['time'], format='%Y-%m-%d'))
    cn10y = np.array(cn_df['10Y'], dtype=float)

    path = os.path.join(interest_rate_dir, 'jgb'+'.csv')
    jp_df = pd.read_csv(path)
    jp_t = pd.DatetimeIndex(pd.to_datetime(jp_df['time'], format='%Y-%m-%d'))
    jp10y = np.array(jp_df['10Y'], dtype=float)

    us_cn_diff_t, us_cn_diff = data_sub(us_t, us10y, cn_t, cn10y)
    us_jp_diff_t, us_jp_diff = data_sub(us_t, us10y, jp_t, jp10y)

    datas = [
             [[[usdcny_t,usdcny,'USDCNY',''],
               ],
               [[us_cn_diff_t,us_cn_diff,'US10Y - CN10Y',''],],''],

             [[[usdjpy_t,usdjpy,'USDJPY',''],
               ],
               [[us_jp_diff_t,us_jp_diff,'US10Y - JP10Y',''],],''],

             [[[us_t,us10y,'US10Y',''],
               [cn_t,cn10y,'CN10Y',''],
               [jp_t,jp10y,'JP10Y',''],
               ],
               [],''],
    ]
    plot_many_figure(datas, start_time='2012-11-01')

    datas = [
             [[[usdcny_t,usdcny,'USDCNY',''],
               ],
               [[usdjpy_t,usdjpy,'USDJPY',''],],''],
    ]
    plot_many_figure(datas, start_time='2012-11-01')


def test5():
    path = os.path.join(data_dir, '利率'+'.csv') 
    df = pd.read_csv(path)
    t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    us10y = np.array(df['美国:国债收益率:10年'], dtype=float)
    eu10y = np.array(df['欧元区:公债收益率:10年'], dtype=float)
    t01, us_eu10y = data_sub(t0, us10y, t0, eu10y)

    path = os.path.join(data_dir, '汇率'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    eurusd = 1/np.array(df['美元兑欧元'], dtype=float)

    fig1 = figure(frame_width=1800, frame_height=300, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, eurusd, line_width=2, line_color='orange', legend_label='EURUSD')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    fig2 = figure(frame_width=1800, frame_height=300, tools=TOOLS, x_axis_type = "datetime", x_range=fig1.x_range, y_axis_location="right")
    fig2.line(t01, us_eu10y, line_width=2, line_color='orange', legend_label='US10Y-EU10Y')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    show(column(fig1,fig2))



def get_central_bank_interest_rate():
    # 美联储
    df = ak.macro_bank_usa_interest_rate()
    prefix = df['商品'][0]
    df.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df.drop(['商品'], axis=1, inplace=True)

    # 欧洲央行
    df1 = ak.macro_bank_euro_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 新西兰联储
    df1 = ak.macro_bank_newzealand_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 中国人民银行
    df1 = ak.macro_bank_china_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 瑞士央行
    df1 = ak.macro_bank_switzerland_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 英国央行
    df1 = ak.macro_bank_english_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 澳洲联储
    df1 = ak.macro_bank_australia_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 俄罗斯
    df1 = ak.macro_bank_russia_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 印度
    df1 = ak.macro_bank_india_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 巴西
    df1 = ak.macro_bank_brazil_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    # 日本
    df1 = ak.macro_bank_japan_interest_rate()
    prefix = df1['商品'][0]
    df1.rename(columns={'日期':'time', '今值':prefix+'_今值', '预测值':prefix+'_预测值', '前值':prefix+'_前值'}, inplace=True)
    df1.drop(['商品'], axis=1, inplace=True)
    df = pd.merge(df, df1, on='time', how='outer')

    df['time'] = df['time'].apply(lambda x:pd.to_datetime(x, format='%Y%m%d'))
    df.sort_values(by = 'time', inplace=True)
    df['time'] = df['time'].apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d'))
    path = os.path.join(data_dir, '各国央行利率决议'+'.csv') 
    df.to_csv(path, encoding='utf-8', index=False)


def plot_central_bank_interest_rate():
    path = os.path.join(data_dir, '各国央行利率决议'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data1 = np.array(df['美联储利率决议_今值'], dtype=float)
    data2 = np.array(df['欧元区利率决议_今值'], dtype=float)
    data3 = np.array(df['中国人民银行利率报告_今值'], dtype=float)
    data4 = np.array(df['澳大利亚利率决议报告_今值'], dtype=float)
    data5 = np.array(df['印度利率决议报告_今值'], dtype=float)
    data6 = np.array(df['英国利率决议报告_今值'], dtype=float)
    data7 = np.array(df['新西兰利率决议报告_今值'], dtype=float)
    data8 = np.array(df['巴西利率决议报告_今值'], dtype=float)
    data9 = np.array(df['日本利率决议报告_今值'], dtype=float)

    datas = [[t, data1, '美联储利率决议_今值'],
             [t, data2, '欧元区利率决议_今值'],
             [t, data3, '中国人民银行利率报告_今值'],
             [t, data4, '澳大利亚利率决议报告_今值'],
             [t, data5, '印度利率决议报告_今值'],
             [t, data6, '英国利率决议报告_今值'],
             [t, data7, '新西兰利率决议报告_今值'],
             [t, data8, '巴西利率决议报告_今值'],
             [t, data9, '日本利率决议报告_今值'],]
    plot_one_figure(datas)


def plot_us_inflation_expectattion_vs_rate():
    path = os.path.join(interest_rate_dir, 'us_yield_curve'+'.csv') 
    df = pd.read_csv(path)
    t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    us10y = np.array(df['10Y'], dtype=float)

    path = os.path.join(interest_rate_dir, 'us_inflation_expectation'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    ie10y = np.array(df['10Y'], dtype=float)

    




if __name__=="__main__":
    plot_china_rate()
    # plot_cny()
    # plot_fx_vs_rate()
    # plot_hk_rate()



    # test2()
    # test5()
    # plot_china_cds()

    # get_central_bank_interest_rate()
    # plot_central_bank_interest_rate()


    pass
