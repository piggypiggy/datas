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

start_time = '2000-1-1'
end_time = '2029-12-31'
delta = pd.Timedelta(days=30)

def test1():
    path = os.path.join(data_dir, 'G4'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    cn = np.array(df['货币当局:总资产'], dtype=float)
    eu = np.array(df['货币当局:总资产'], dtype=float)

    pmi_eu_mf = np.array(df['欧元区:制造业PMI'], dtype=float)
    pmi_cn_mf = np.array(df['PMI'], dtype=float)
    pmi_us_mf = np.array(df['美国:供应管理协会(ISM):PMI:季调'], dtype=float)
    t11, pmi_eu_mf = get_period_data(t1, pmi_eu_mf, start_time, end_time, remove_nan=True)
    t12, pmi_cn_mf = get_period_data(t1, pmi_cn_mf, start_time, end_time, remove_nan=True)
    t12, pmi_us_mf = get_period_data(t1, pmi_us_mf, start_time, end_time, remove_nan=True)

    path = os.path.join(data_dir, '货币供应量'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    eu_m1_yoy = np.array(df['欧元区:M1:季调:同比'], dtype=float)
    cn_m1_yoy = np.array(df['M1:同比'], dtype=float)
    us_m1_yoy = np.array(df['美国:M1:季调:当月同比'], dtype=float)
    t21, eu_m1_yoy = get_period_data(t2, eu_m1_yoy, start_time, end_time, remove_nan=True)
    t22, cn_m1_yoy = get_period_data(t2, cn_m1_yoy, start_time, end_time, remove_nan=True)
    t23, us_m1_yoy = get_period_data(t2, us_m1_yoy, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1400, frame_height=400, x_axis_type = "datetime")
    fig1.y_range = Range1d(np.min(eu_m1_yoy)-5, np.max(eu_m1_yoy)+5)
    fig1.line(t11, eu_m1_yoy, line_width=3, line_color='orange', legend_label='欧元区:制造业PMI 左轴')

    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.nanmin(eu_m1_yoy)*1.05,
            end=np.nanmax(eu_m1_yoy)*1.05,
        ),
    }

    fig1.line(t21+delta*0, eu_m1_yoy, line_width=2, line_color='lightgray', y_range_name=y_column2_name, legend_label='欧元区:M1:季调:同比 右轴')
    fig1.line(t21+delta*9, eu_m1_yoy, line_width=3, line_color='blue', y_range_name=y_column2_name, legend_label='欧元区:M1:季调:同比 右移9个月 右轴')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left' 
    show(fig1)


# 流动性 
# TODO: us bank reserve at fed
def test2():
    path = os.path.join(data_dir, 'G4'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    path = os.path.join(fx_dir, 'USDCNH'+'.csv') 
    df1 = pd.read_csv(path)
    t_usd_cnh = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    usd_cnh = np.array(df1['close'], dtype=float)

    path = os.path.join(fx_dir, 'EURUSD'+'.csv') 
    df1 = pd.read_csv(path)
    t_eur_usd = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    eur_usd = np.array(df1['close'], dtype=float)

    path = os.path.join(fx_dir, 'USDJPY'+'.csv') 
    df1 = pd.read_csv(path)
    t_usd_jpy = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    usd_jpy = np.array(df1['close'], dtype=float)


    # billion
    us_total = np.array(df['美国:所有联储银行:总负债'], dtype=float) / 1000  # million -> billion
    us_onrrp = np.array(df['美国:所有联储银行:逆回购协议'], dtype=float) / 1000
    us_tga = np.array(df['美国:存款机构:财政部现金存款'], dtype=float) / 1000
    t1, us_total = interpolate_nan(t, us_total)
    _, us_onrrp = interpolate_nan(t, us_onrrp)
    _, us_tga = interpolate_nan(t, us_tga)
    us_liquidity = us_total - us_onrrp - us_tga

    cn_total = np.array(df['货币当局:总资产'], dtype=float) / 10  # 亿 -> 十亿
    t2, cn_total = interpolate_nan(t, cn_total)
    t21, cn_total1 = data_div(t2, cn_total, t_usd_cnh, usd_cnh)

    eu_total = np.array(df['欧洲央行:资产:总额'], dtype=float) / 1000  # million -> billion
    t3, eu_total = interpolate_nan(t, eu_total)
    t31, eu_total1 = data_mul(t3, eu_total, t_eur_usd, eur_usd)

    jp_total = np.array(df['日本央行:资产:总额'], dtype=float) / 1000000  # 千 -> 十亿
    t4, jp_total = interpolate_nan(t, jp_total)
    t41, jp_total1 = data_div(t4, jp_total, t_usd_jpy, usd_jpy)

    t5, g4_total = data_add(t1, us_liquidity, t21, cn_total1)
    t5, g4_total = data_add(t5, g4_total, t31, eu_total1)
    t5, g4_total = data_add(t5, g4_total, t41, jp_total1)    

    # https://www.zerohedge.com/the-market-ear/follow-liquidity
    fig1 = figure(frame_width=1400, frame_height=250, tools=TOOLS, x_axis_type = "datetime")
    fig1.line(t1, us_liquidity, line_width=2, line_color='orange', legend_label='US = BALANCE SHEET - ONRRP - TGA ($bn)') 
    fig1.line(t21, cn_total1, line_width=2, line_color='red', legend_label='CN ($bn)') 
    fig1.line(t31, eu_total1, line_width=2, line_color='blue', legend_label='EU ($bn)') 
    fig1.line(t41, jp_total1, line_width=2, line_color='darkgreen', legend_label='JP ($bn)') 
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left' 

    fig2 = figure(frame_width=1400, frame_height=250, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime")
    fig2.line(t5, g4_total, line_width=2, line_color='black', legend_label='G4 LIQUIDITY ($bn)') 
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"
    fig2.legend.location='top_left' 

    show(column(fig1,fig2))


    fig1 = figure(frame_width=1400, frame_height=175, tools=TOOLS, x_axis_type = "datetime")
    fig1.line(t1, us_liquidity, line_width=2, line_color='orange', legend_label='US = BALANCE SHEET - ONRRP - TGA ($bn)') 
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left' 

    fig2 = figure(frame_width=1400, frame_height=175, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime")
    fig2.line(t2, cn_total, line_width=2, line_color='orange', legend_label='CN (bn)') 
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"
    fig2.legend.location='top_left' 

    fig3 = figure(frame_width=1400, frame_height=175, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime")
    fig3.line(t3, eu_total, line_width=2, line_color='orange', legend_label='EU (bn)') 
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.click_policy="hide"
    fig3.legend.location='top_left' 

    fig4 = figure(frame_width=1400, frame_height=175, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime")
    fig4.line(t4, jp_total, line_width=2, line_color='orange', legend_label='JP (bn)') 
    fig4.xaxis[0].ticker.desired_num_ticks = 20
    fig4.legend.click_policy="hide"
    fig4.legend.location='top_left' 

    show(column(fig1,fig2,fig3,fig4))


    path = os.path.join(msci_dir, 'WORLD.csv')
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    world = np.array(df['level_eod'], dtype=float)

    datas = [
             [[[t1,world,'MSCI WORLD',''],],[[t5,g4_total,'G4 LIQUIDITY ($bn)','']],''],]

    plot_many_figure(datas, start_time='2011-01-01')

# test1()

test2()
