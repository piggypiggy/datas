import os
import time

import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d
from scipy.stats import linregress
from utils import *
from cftc import *


def test2():
    path = os.path.join(cfd_dir, 'GOLD CFD'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['close'], dtype=float)
    cftc_plot_disaggregated(t, data, 'GOLD CFD', code='088691', inst_name='COMEX:黄金')


# 黄金储备
def test3():
    start_time = '2000-01-01'
    end_time = '2029-12-31'

    path = os.path.join(data_dir, '金银'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    pboc_stock = np.array(df['黄金储备'], dtype=float)
    gold_price_usd = np.array(df['期货收盘价:COMEX黄金'], dtype=float)
    gold_price_cny = np.array(df['期货收盘价(主力):黄金'], dtype=float)
    t1, pboc_stock = get_period_data(t, pboc_stock, start_time, end_time, remove_nan=True)
    t2, gold_price_usd = get_period_data(t, gold_price_usd, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1800, frame_height=500, x_axis_type = "datetime")
    fig1.y_range = Range1d(np.min(pboc_stock)*0.9, np.max(pboc_stock)*1.1)
    fig1.line(t1, pboc_stock, line_width=2, line_color='red', legend_label='人民银行 黄金储备 万盎司 左轴')

    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(gold_price_usd)*0.9,
            end=np.max(gold_price_usd)*1.1,
        ),
    }
    fig1.line(t2, gold_price_usd, line_width=2, color='blue', y_range_name=y_column2_name, legend_label='黄金价格 美元 右轴')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.location='top_left' 
    show(fig1)


# 白银持仓
def test4():
    path = os.path.join(cfd_dir, 'SILVER CFD'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['close'], dtype=float)
    cftc_plot_disaggregated(t, data, 'SILVER CFD', code='084691', inst_name='COMEX:白银')


def test5():
    path = os.path.join(cfd_dir, 'GOLD CFD'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data1 = np.array(df['close'], dtype=float)

    path = os.path.join(fx_dir, 'USDIDX'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data2 = np.array(df['close'], dtype=float)

    cftc_plot_financial(t2, data2, '美元指数', t1, data1, 'GOLD CFD', code='098662', inst_name='ICE:美元指数')


start_time = '2009-1-1'
end_time = '2029-10-10'
# VOLATILITY
def test8():
    path = os.path.join(cfd_dir, 'GOLD CFD'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data1 = np.array(df['close'], dtype=float)

    path = os.path.join(data_dir, 'vix'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    gvx = np.array(df['gold_etf_vix'], dtype=float)

    fig1 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime")
    fig1.line(t1, data1, line_width=2, line_color='black', legend_label='gold')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", x_range=fig1.x_range)
    fig2.line(t2, gvx, line_width=2, line_color='black', legend_label='gold etf volatility')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    show(column(fig1,fig2))

# EURUSD
def test9():
    path = os.path.join(cfd_dir, 'GOLD CFD'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data1 = np.array(df['close'], dtype=float)

    path = os.path.join(fx_dir, 'EURUSD'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data2 = np.array(df['close'], dtype=float)

    cftc_plot_financial(t2, data2, 'EURUSD', t1, data1, 'GOLD CFD', code='099741', inst_name='CME:EURUSD')


# GOLD/CNY 和 GOLD/USD
def test10():
    path = os.path.join(cfd_dir, 'GOLD CFD'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    gold_price_usd = np.array(df['close'], dtype=float)
    t2, gold_price_usd = get_period_data(t2, gold_price_usd, start_time, end_time, remove_nan=True)

    path = os.path.join(future_price_dir, 'shfe', 'au'+'.csv') 
    df = pd.read_csv(path, header=[0,1])
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    gold_price_cny = np.array(df['index']['close'], dtype=float)
    t1, gold_price_cny = get_period_data(t1, gold_price_cny, start_time, end_time, remove_nan=True)

    path = os.path.join(fx_dir, 'USDCNY'+'.csv') 
    df1 = pd.read_csv(path)
    dxy_t = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    usdcny = np.array(df1['close'], dtype=float)

    gold_price_cny = gold_price_cny * 31.103481
    t4, real_usd_cny = data_div(t1, gold_price_cny, t2, gold_price_usd)
    t1, gold_price_cny_to_usd = data_div(t1, gold_price_cny, dxy_t, usdcny)

    t3, diff = data_sub(t1, gold_price_cny_to_usd, t2, gold_price_usd)

    # t4, real_usd_cny = data_div(t1, gold_price_cny_to_usd, t2, gold_price_usd)

    fig1 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, gold_price_cny_to_usd, line_width=2, line_color='blue', legend_label='gold cny to usd')
    fig1.line(t2, gold_price_usd, line_width=2, line_color='orange', legend_label='gold usd')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", x_range=fig1.x_range, y_axis_location="right")
    fig2.line(t3, diff, line_width=2, line_color='blue', legend_label='CNY 溢价')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"


    fig3 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", x_range=fig1.x_range, y_axis_location="right")
    fig3.line(t4, real_usd_cny, line_width=2, line_color='blue', legend_label='REAL USDCNY')
    fig3.line(dxy_t, usdcny, line_width=2, line_color='orange', legend_label='USDCNY')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.click_policy="hide"

    show(column(fig1,fig2,fig3))




if __name__=="__main__":
    # # 黄金持仓
    # test2()

    # # # 黄金储备
    # # # test3()

    # # # # 白银持仓
    # test4()

    # # # # # 美元指数
    # test5()

    # # # # VOLATILITY
    # test8()

    # # # # EURUSD
    # # test9()

    # # SOFR 3M
    # # test8()

    # # GOLD/CNY 和 GOLD/USD
    # test10()


    pass

