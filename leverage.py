import os
import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column, grid
from bokeh.plotting import figure
from utils import *



start_time = '2005-1-1'
end_time = '2024-12-31'


# 宏观杠杆率
def test1():
    path = os.path.join(data_dir, '杠杆率'+'.csv') 
    df = pd.read_csv(path)

    # df_gdp = df[['time','GDP:当季值','预测值:GDP:当季同比']].dropna()
    # t = pd.DatetimeIndex(pd.to_datetime(df_gdp['time'], format='%Y-%m-%d'))
    # gdp_season = np.array(df_gdp['GDP:当季值'], dtype=float)
    # gdp_season_yoy_predict = np.array(df_gdp['预测值:GDP:当季同比'], dtype=float)

    # gdp_season_t, gdp_season = interpolate_season_to_month(t, gdp_season)
    # print(gdp_season)
    # gdp_season_predict = np.zeros(gdp_season.shape, dtype=float)

    # # 根据'预测值:GDP:当季同比'估计的一个季度内每个月的GDP,(x-12至x月的总和)
    # for i in range(3,len(gdp_season_yoy_predict)):
    #     idx = np.where(gdp_season_t == t[i])[0][0]
    #     tmp = gdp_season[idx-12]*(1+gdp_season_yoy_predict[i]/100)
    #     gdp_season_predict[idx] = tmp + gdp_season[idx-3] + gdp_season[idx-6] + gdp_season[idx-9]
    #     gdp_season_predict[idx-1] = (gdp_season[idx-1]*1+tmp*2)/3 + gdp_season[idx-4] + gdp_season[idx-7] + gdp_season[idx-10]
    #     gdp_season_predict[idx-2] = (gdp_season[idx-1]*2+tmp*1)/3 + gdp_season[idx-5] + gdp_season[idx-8] + gdp_season[idx-11]
    #     # print(t[i], gdp_season_predict[idx])
    
    # df2 = df[['time','社会融资规模存量:贷款核销','社会融资规模存量:存款类金融机构资产支持证券','社会融资规模存量:企业债券',
    # '社会融资规模存量:未贴现的银行承兑汇票','社会融资规模存量:信托贷款','社会融资规模存量:委托贷款','社会融资规模存量:外币贷款(折合人民币)',
    # '金融机构:人民币:境内贷款:非金融企业及机关团体','金融机构:人民币:境内贷款:住户']].dropna()

    # t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    # a1 = np.array(df2['社会融资规模存量:贷款核销'], dtype=float)
    # a2 = np.array(df2['社会融资规模存量:存款类金融机构资产支持证券'], dtype=float)
    # a3 = np.array(df2['社会融资规模存量:企业债券'], dtype=float) * 10000
    # a4 = np.array(df2['社会融资规模存量:未贴现的银行承兑汇票'], dtype=float) * 10000
    # a5 = np.array(df2['社会融资规模存量:信托贷款'], dtype=float) * 10000
    # a6 = np.array(df2['社会融资规模存量:委托贷款'], dtype=float) * 10000  
    # a7 = np.array(df2['社会融资规模存量:外币贷款(折合人民币)'], dtype=float) * 10000  
    # a8 = np.array(df2['金融机构:人民币:境内贷款:非金融企业及机关团体'], dtype=float)
    # non_financial = a1+a2+a3+a4+a5+a6+a7+a8

    # t11, non_financial_leverage_predict = data_div(t2, non_financial, gdp_season_t, gdp_season_predict)
    # non_financial_leverage_predict *= 100

    # a9 = np.array(df2['金融机构:人民币:境内贷款:住户'], dtype=float)
    # t12, resident_leverage_predict = data_div(t2, a9, gdp_season_t, gdp_season_predict)
    # resident_leverage_predict *= 100

    df3 = df[['time','非金融企业部门债务占GDP比重(非金融企业部门杠杆率)','居民部门债务占GDP比重(居民部门杠杆率)']].dropna()
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m'))
    non_financial_leverage = np.array(df3['非金融企业部门债务占GDP比重(非金融企业部门杠杆率)'], dtype=float)
    resident_leverage = np.array(df3['居民部门债务占GDP比重(居民部门杠杆率)'], dtype=float)


    fig1 = figure(frame_width=1400, frame_height=300, x_axis_type = "datetime")
    # fig1.line(t11, non_financial_leverage_predict, line_width=2, line_color='orange', legend_label=' 预测')
    # fig1.circle(t11, non_financial_leverage_predict, color='orange', legend_label=' 预测')
    fig1.line(t3, non_financial_leverage, line_width=2, line_color='blue', legend_label='non_financial_leverage')
    fig1.circle(t3, non_financial_leverage, color='blue', legend_label='non_financial_leverage')
    fig1.xaxis[0].ticker.desired_num_ticks = 20

    fig2 = figure(frame_width=1400, frame_height=300, x_range=fig1.x_range, x_axis_type = "datetime")
    # fig2.line(t12, resident_leverage_predict, line_width=2, line_color='orange', legend_label=' 预测')
    # fig2.circle(t12, resident_leverage_predict, color='orange', legend_label=' 预测')
    fig2.line(t3, resident_leverage, line_width=2, line_color='blue', legend_label='resident_leverage')
    fig2.circle(t3, resident_leverage, color='blue', legend_label='resident_leverage')
    fig2.xaxis[0].ticker.desired_num_ticks = 20

    show(column(fig1,fig2))



test1()
