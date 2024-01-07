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

start_time = '2012-1-1'
end_time = '2025-12-31'
delta = pd.Timedelta(days=30)

def test1():
    path = os.path.join(data_dir, '中美PMI'+'.csv') 
    df = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    pmi_eu_mf = np.array(df['欧元区:制造业PMI'], dtype=float)
    pmi_cn_mf = np.array(df['PMI'], dtype=float)
    pmi_us_mf = np.array(df['美国:供应管理协会(ISM):PMI:季调'], dtype=float)
    t11, pmi_eu_mf = get_period_data(t1, pmi_eu_mf, start_time, end_time, remove_nan=True)
    t12, pmi_cn_mf = get_period_data(t1, pmi_cn_mf, start_time, end_time, remove_nan=True)
    t12, pmi_us_mf = get_period_data(t1, pmi_us_mf, start_time, end_time, remove_nan=True)

    path = os.path.join(data_dir, '货币供应量'+'.csv') 
    df = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
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


def test2():
    path = os.path.join(data_dir, '中美PMI'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    # 中国制造业
    names = ['PMI','PMI:生产','PMI:新订单','PMI:新出口订单','PMI:积压订单','PMI:产成品库存','PMI:采购量','PMI:进口','PMI:购进价格','PMI:原材料库存','PMI:从业人员','PMI:供应商配送时间','PMI:生产经营活动预期','PMI:出厂价格']
    datas = []
    for i in range(len(names)):
        datas.append([t, np.array(df[names[i]], dtype=float), names[i]])

    plot_one_figure(datas, '', '2000-01-01', '2100-01-01')

    # 美国制造业
    names = ['美国:供应管理协会(ISM):PMI:季调','美国:ISM:PMI:新订单:季调','美国:ISM:PMI:产出:季调','美国:ISM:PMI:就业:季调','美国:ISM:PMI:供应商交付:季调','美国:ISM:PMI:自有库存:季调','美国:ISM:PMI:客户库存:季调','美国:ISM:PMI:物价:季调','美国:ISM:PMI:订单库存:季调','美国:ISM:PMI:新出口订单:季调','美国:ISM:PMI:进口:季调']
    datas = []
    for i in range(len(names)):
        datas.append([t, np.array(df[names[i]], dtype=float), names[i]])

    plot_one_figure(datas, '', '2000-01-01', '2100-01-01')

    # 美国服务业
    names = ['美国:供应管理协会(ISM):服务业PMI:季调','美国:ISM:服务业PMI:商业活动:季调','美国:ISM:服务业PMI:新订单:季调','美国:ISM:服务业PMI:就业:季调','美国:ISM:服务业PMI:供应商交付:季调','美国:ISM:服务业PMI:库存:季调','美国:ISM:服务业PMI:物价:季调','美国:ISM:服务业PMI:订单库存:季调','美国:ISM:服务业PMI:新出口订单:季调','美国:ISM:服务业PMI:进口:季调','美国:ISM:服务业PMI:库存景气:季调']
    datas = []
    for i in range(len(names)):
        datas.append([t, np.array(df[names[i]], dtype=float), names[i]])

    plot_one_figure(datas, '', '2000-01-01', '2100-01-01')
    # pmi1 = np.array(df['PMI:生产'], dtype=float)
    # pmi2 = np.array(df['PMI:新订单'], dtype=float)
    # pmi3 = np.array(df['PMI:新出口订单'], dtype=float)
    # pmi4 = np.array(df['PMI:积压订单'], dtype=float)
    # pmi5 = np.array(df['PMI:产成品库存'], dtype=float)
    # pmi6 = np.array(df['PMI:采购量'], dtype=float)
    # pmi7 = np.array(df['PMI:进口'], dtype=float)
    # pmi8 = np.array(df['PMI:购进价格'], dtype=float)
    # pmi9 = np.array(df['PMI:原材料库存'], dtype=float)
    # pmi10 = np.array(df['PMI:从业人员'], dtype=float)
    # pmi11 = np.array(df['PMI:供应商配送时间'], dtype=float)
    # pmi12 = np.array(df['PMI:生产经营活动预期'], dtype=float)
    # pmi13 = np.array(df['PMI:出厂价格'], dtype=float)
    # t1, pmi1 = get_period_data(t, pmi1, start_time, end_time, remove_nan=True)
    # t2, pmi2 = get_period_data(t, pmi2, start_time, end_time, remove_nan=True)
    # t3, pmi3 = get_period_data(t, pmi3, start_time, end_time, remove_nan=True)
    # t4, pmi4 = get_period_data(t, pmi4, start_time, end_time, remove_nan=True)
    # t5, pmi5 = get_period_data(t, pmi5, start_time, end_time, remove_nan=True)
    # t6, pmi6 = get_period_data(t, pmi6, start_time, end_time, remove_nan=True)
    # t7, pmi7 = get_period_data(t, pmi7, start_time, end_time, remove_nan=True)
    # t8, pmi8 = get_period_data(t, pmi8, start_time, end_time, remove_nan=True)
    # t9, pmi9 = get_period_data(t, pmi9, start_time, end_time, remove_nan=True)
    # t10, pmi10 = get_period_data(t, pmi10, start_time, end_time, remove_nan=True)
    # t11, pmi11 = get_period_data(t, pmi11, start_time, end_time, remove_nan=True)
    # t12, pmi12 = get_period_data(t, pmi12, start_time, end_time, remove_nan=True)
    # t13, pmi13 = get_period_data(t, pmi13, start_time, end_time, remove_nan=True)




# test1()

test2()
