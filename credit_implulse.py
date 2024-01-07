import os
import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d, VBar, NumeralTickFormatter
from scipy.stats import linregress
from utils import *
from cftc import *


month1 = pd.Timedelta(days=30)

def credit_impulse():
    path = os.path.join(nbs_dir, 'GDP'+'.csv') 
    gdp_season_df = pd.read_csv(path)
    gdp_season_time = pd.DatetimeIndex(pd.to_datetime(gdp_season_df['time'], format='%Y-%m'))
    gdp_season_data = np.array(gdp_season_df['国内生产总值_当季值'], dtype=float)

    # 3个月内的gdp总和
    gdp_month_time, gdp_month_data = interpolate_season_to_month(gdp_season_time, gdp_season_data)
    gdp_year_data = gdp_month_data.copy()
    gdp_year_data[12:] += gdp_month_data[0:-12]
    gdp_year_data[12:] += gdp_month_data[4:-8]
    gdp_year_data[12:] += gdp_month_data[8:-4]
    gdp_year_data = gdp_year_data[12:]
    gdp_year_time = gdp_month_time[12:]

    # gdp_year_data = gdp_season_data.copy()
    # gdp_year_data[1:] += gdp_season_data[:-1]
    # gdp_year_data[2:] += gdp_season_data[:-2]
    # gdp_year_data[3:] += gdp_season_data[:-3]
    # gdp_year_data = gdp_year_data[4:]
    # gdp_year_time = gdp_season_time[4:]


    path = os.path.join(pboc_dir, '社会融资规模'+'.csv') 
    sf_month_df = pd.read_csv(path)
    sf_month_time = pd.DatetimeIndex(pd.to_datetime(sf_month_df['time']))
    sf_month_data = np.array(sf_month_df['社会融资规模增量:企业债券'], dtype=float)
                    # np.array(sf_month_df['社会融资规模:非金融企业境内股票融资:当月值'], dtype=float) 

    # 季度新增融资求和
    sf_season_data = sf_month_data.copy()
    sf_season_data[2:] += sf_month_data[0:-2]
    sf_season_data[2:] += sf_month_data[1:-1]
    sf_season_time = sf_month_time[2:]
    sf_season_data = sf_season_data[2:]

    # 半年新增融资求和
    sf_halfyear_data = sf_month_data.copy()
    for i in range(6-1):
        sf_halfyear_data[5:] += sf_month_data[i:i-5]
    sf_halfyear_time = sf_month_time[5:]
    sf_halfyear_data = sf_halfyear_data[5:]

    # 年新增融资求和
    sf_year_data = sf_month_data.copy()
    for i in range(12-1):
        sf_year_data[11:] += sf_month_data[i:i-11]
    sf_year_time = sf_month_time[11:]
    sf_year_data = sf_year_data[11:]

    # 铁矿石
    # path = os.path.join(data_dir, '铁矿'+'.csv') 
    # i_df = pd.read_csv(path)
    # i_time = pd.DatetimeIndex(pd.to_datetime(i_df['time'], format='%Y-%m-%d'))
    # i_data = np.array(i_df['期货收盘价:铁矿石:连三'], dtype=float)

    path = os.path.join(future_price_dir, 'dce', 'i'+'.csv')
    i_df = pd.read_csv(path, header=[0,1])
    i_time = pd.DatetimeIndex(pd.to_datetime(i_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    i_data = np.array(i_df.loc[:, pd.IndexSlice['index', 'close']], dtype=float)

    i_time, i_data = interpolate_nan(i_time, i_data)
    i_time_yoy, i_data_yoy = yoy(i_time, i_data)
    # print(i_time)

    # 同步时间
    gdp_idx = np.isin(gdp_year_time, sf_year_time)
    sf_idx = np.isin(sf_year_time, gdp_year_time)

    gdp_year_time = gdp_year_time[gdp_idx]
    gdp_year_data = gdp_year_data[gdp_idx]

    sf_year_time = sf_year_time[sf_idx]
    sf_year_data = sf_year_data[sf_idx]

    # 信贷脉冲
    ratio = sf_year_data/gdp_year_data
    res = ratio.copy()
    res[12:] = ratio[12:] - ratio[:-12]
    res[0:12] = 0

    res = res.astype(np.float32)
    sf_year_time, res = interpolate_nan(sf_year_time, res)
    sf_year_time1, res1 = get_period_data(sf_year_time+month1*6, res, "2017-01-01", "2099-01-01", remove_nan=True)
    sf_year_time, res = get_period_data(sf_year_time, res, "2017-01-01", "2099-01-01", remove_nan=True)
    i_time_yoy1, i_data_yoy1 = get_period_data(i_time_yoy, i_data_yoy, "2017-01-01", "2099-01-01", remove_nan=True)
    # plot
    fig1 = figure(frame_width=1400, frame_height=350, tools=TOOLS, x_axis_type = "datetime")
    # fig1.line(sf_year_time+delta*0, res, line_width=2, line_color='red', legend_label='信贷脉冲:企业债券融资')
    fig1.line(sf_year_time, res, line_width=3, line_color='lightgray', legend_label='信贷脉冲:企业债券融资 左轴')
    fig1.line(sf_year_time1, res1, line_width=3, line_color='red', legend_label='信贷脉冲:企业债券融资 右移6个月 左轴')
    fig1.line(sf_year_time1, 0, line_width=3, line_color='black')

    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.yaxis[0].ticker.desired_num_ticks = 20
    fig1.yaxis[0].formatter = NumeralTickFormatter(format='0.0%')
    # fig1.y_range = Range1d(start = -0.025, end = 0.05)
    fig1.y_range = Range1d(start = np.min(res)*1.05, end = np.max(res)*1.05)

    fig1.extra_y_ranges = {"y2": Range1d(start = -0.5, end = 1.25)}  
    fig1.add_layout(LinearAxis(y_range_name = "y2"), 'right')
    fig1.line(i_time_yoy1, i_data_yoy1, line_width=2, line_color = "blue", y_range_name = "y2", legend_label='铁矿石指数 价格同比 右轴')



    # 用信贷脉冲预测铁矿价格
    # idx1 = np.isin(i_time_yoy1, sf_year_time1)
    # i_data_yoy1 = i_data_yoy1[idx1]
    # plot_circle(i_data_yoy1, res1, legend='')

    # 从哪天开始预测
    i_time, i_data = get_period_data(i_time, i_data, "2017-01-01", "2099-01-01", remove_nan=True)
    # start_time = i_time[-1] + pd.Timedelta(days=1)
    days = 30
    start_time = i_time[-1] - pd.Timedelta(days=days-1)
    start_idx = len(i_time) - days
    res2 = res1[start_idx-730:start_idx]
    i_data_yoy1 = i_data_yoy1[start_idx-730-365:start_idx-365]

    L = (sf_year_time1[-1] - start_time).days + 1
    print('L=',L)
    t1 = pd.date_range(start=start_time, end=start_time+pd.Timedelta(days=L-1))
    up = np.zeros((L), dtype=float)
    avg = np.zeros((L), dtype=float)
    med = np.zeros((L), dtype=float)
    down = np.zeros((L), dtype=float)
    for i in range(L):
        delta = 0.00025
        while (1):
            idx = np.where(((res1[start_idx+i]-delta) <= res2) & (res2 <= (res1[start_idx+i]+delta)))[0]
            if (len(idx) > 50):
                break
            delta = delta + 0.00025

        dist = i_data_yoy1[idx]
        up[i] = i_data[-365-days+i+1] * (1 + np.percentile(dist, 75))
        avg[i] = i_data[-365-days+i+1] * (1 + np.average(dist))
        med[i] = i_data[-365-days+i+1] * (1 + np.median(dist))
        down[i] = i_data[-365-days+i+1] * (1 + np.percentile(dist, 25))

    fig2 = figure(frame_width=1400, frame_height=350, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime")  
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.yaxis[0].ticker.desired_num_ticks = 20
    fig2.y_range = Range1d(start = np.min(i_data)*0.95, end = np.max(i_data)*1.05)
    fig2.line(i_time, i_data, line_width=2, line_color = "black", legend_label='铁矿石指数 价格')
    fig2.line(t1, up, line_width=2, line_color = "red", legend_label='铁矿石指数 价格预测 75%')
    fig2.line(t1, avg, line_width=2, line_color = "gray", legend_label='铁矿石指数 价格预测 avg')
    fig2.line(t1, med, line_width=2, line_color = "deeppink", legend_label='铁矿石指数 价格预测 median')
    fig2.line(t1, down, line_width=2, line_color = "darkgreen", legend_label='铁矿石指数 价格预测 25%')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    show(column(fig1,fig2))

# 期现价差
def test1():
    plot_basis('dce', 'i', adjust=1/0.9)


if __name__=="__main__":
    credit_impulse()

    # test1()



