import os
import time

import numpy as np
import pandas as pd
import datetime
from bokeh.io import output_file, show
from bokeh.layouts import column, grid
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d
from scipy.stats import linregress
from chinese_calendar import is_workday, is_holiday
from utils import *
from chinese_calendar import is_workday
from cftc import *
from lme import *
from option import *

cur_dir = os.path.dirname(__file__)
data_dir = os.path.join(cur_dir, 'data')

start_time = '2005-1-1'
end_time = '2028-12-31'

# 20种颜色
many_colors = ['crimson','orange','blue','darkgreen','khaki','purple','deeppink',
                'cyan','darkgray','tomato','turquoise','yellow','yellowgreen','gold','black',
                'teal','midnightblue','red','gold']

def test1():
    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    shfe_cu = np.array(df['期货收盘价(主力):阴极铜'], dtype=float)

    t00, lme_cu_3m = get_period_data(t, lme_cu_3m, start_time, end_time, remove_nan=True)
    t01, shfe_cu = get_period_data(t, shfe_cu, start_time, end_time, remove_nan=True)

    cftc_plot_disaggregated(t00, lme_cu_3m, '期货收盘价:LME3个月铜', t01, shfe_cu, '期货收盘价(主力):沪铜', code='085692', inst_name='CME:铜')


def test11():
    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    shfe_cu = np.array(df['期货收盘价(主力):阴极铜'], dtype=float)

    t00, lme_cu_3m = get_period_data(t, lme_cu_3m, start_time, end_time, remove_nan=True)
    t01, shfe_cu = get_period_data(t, shfe_cu, start_time, end_time, remove_nan=True)

    lme_plot_position(t00, lme_cu_3m, '期货收盘价:LME3个月铜', t01, shfe_cu, '期货收盘价(主力):沪铜', code='Copper', inst_name='LME:铜')


# 铜库存
def test2():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)

    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    shfe_cu = np.array(df1['期货收盘价(主力):阴极铜'], dtype=float)
    lme_cu_03 = np.array(df1['LME铜升贴水(0-3)'], dtype=float)
    # sh_bonded_area_stock1 = np.array(df1['库存期货：铜：保税商品总计'], dtype=float)
    # # sh_bonded_area_stock2 = np.array(df1['库存:铜:上海保税区'], dtype=float)
    lme_stock = np.array(df1['LME总库存:铜'], dtype=float)
    lme_stock_cancel = np.array(df1['LME注销仓单:铜'], dtype=float)
    lme_stock_register = np.array(df1['LME注册仓单:铜'], dtype=float)

    sh_stock = np.array(df1['库存:阴极铜'], dtype=float)
    # premium1 = np.array(df1['1#电解铜升贴水:最大值'], dtype=float)
    comex_stock = np.array(df1['COMEX库存:铜'], dtype=float)

    
    
    # idx = np.logical_not(np.isnan(sh_bonded_area_stock1))
    # t11, sh_bonded_area_stock1 = get_period_data(t1[idx], sh_bonded_area_stock1[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(lme_stock))
    t13, lme_stock = get_period_data(t1[idx], lme_stock[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(sh_stock))
    t14, sh_stock = get_period_data(t1[idx], sh_stock[idx], start_time, end_time)
    t15, comex_stock = get_period_data(t1, comex_stock, start_time, end_time, remove_nan=True)

    idx = np.logical_not(np.isnan(shfe_cu))
    t01 = t1[idx]
    shfe_cu = shfe_cu[idx]
    fig0 = figure(frame_width=1400, frame_height=200, tools=TOOLS, x_axis_type = "datetime")
    fig0.line(t01, shfe_cu, line_width=2, line_color='black', legend_label='沪铜')
    fig0.xaxis[0].ticker.desired_num_ticks = 20


    fig1 = figure(frame_width=1400, frame_height=250, tools=TOOLS, x_range=fig0.x_range, x_axis_type = "datetime")
    # fig1.line(t11, sh_bonded_area_stock1, color='orange', legend_label='库存:铜:保税')    
    fig1.line(t13, lme_stock, line_width=2, color='blue', legend_label='总库存:LME铜')   
    fig1.line(t14, sh_stock, line_width=2, color='deeppink', legend_label='库存期货:阴极铜')  
    fig1.line(t15, comex_stock, line_width=2, color='darkgreen', legend_label='COMEX库存:铜')  
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.yaxis[0].axis_label = '库存(吨)'

    # 库存、仓单、持仓
    idx = np.logical_not(np.isnan(lme_stock_register))
    t16 = t1[idx]
    lme_stock_register = lme_stock_register[idx]
    idx = np.logical_not(np.isnan(lme_stock_cancel))
    t17 = t1[idx]
    lme_stock_cancel = lme_stock_cancel[idx]

    fig2 = figure(frame_width=1400, frame_height=250, tools=TOOLS, x_range=fig0.x_range, x_axis_type = "datetime")
    # fig2.line(tp_short, lme_position_short, line_width=2, line_color='red', legend_label='LME 持仓量')
    fig2.line(t13, lme_stock, line_width=1, line_color='blue', legend_label='LME 库存')
    fig2.line(t16, lme_stock_register, line_width=1, line_color='orange', legend_label='LME 注册仓单')
    fig2.line(t17, lme_stock_cancel, line_width=1, line_color='darkgreen', legend_label='LME 注销仓单')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    # # 库存、仓单/持仓
    # fig3 = figure(frame_width=1400, frame_height=250, x_range=fig0.x_range, x_axis_type = "datetime")
    # fig3.line(tr1, ratio1, line_width=2, line_color='orange', legend_label='LME 库存/持仓量')
    # fig3.line(tr2, ratio2, line_width=2, line_color='blue', legend_label='LME 注册仓单/持仓量')
    # fig3.line(tr3, ratio3, line_width=2, line_color='darkgreen', legend_label='LME 注销仓单/持仓量')
    # fig3.y_range = Range1d(0, 1.1)

    # idx = np.logical_not(np.isnan(lme_cu_03))
    # t21 = t1[idx]
    # lme_cu_03 = lme_cu_03[idx]
    # y_column2_name = 'y22'
    # fig3.extra_y_ranges = {
    #     y_column2_name: Range1d(
    #         start=np.min(lme_cu_03) - abs(np.min(lme_cu_03))*0.1,
    #         end=200,
    #     ),
    # }
    # fig3.line(t21, lme_cu_03, color='black', y_range_name=y_column2_name, legend_label='LME 0-3')
    # fig3.add_layout(LinearAxis(y_range_name=y_column2_name), 'right')
    # fig3.xaxis[0].ticker.desired_num_ticks = 20
    # fig3.legend.click_policy="hide"

    show(column(fig0,fig1,fig2))


# 铜 PMI
def test3():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df1['期货收盘价:LME3个月铜'], dtype=float)
    shfe_cu = np.array(df1['期货收盘价(主力):阴极铜'], dtype=float)

    path2 = os.path.join(data_dir, '中美PMI'+'.csv') 
    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m'))
    pmi = np.array(df2['PMI'], dtype=float)
    pmi1 = np.array(df2['PMI:生产'], dtype=float)
    pmi2 = np.array(df2['PMI:新订单'], dtype=float)
    pmi3 = np.array(df2['PMI:新出口订单'], dtype=float)
    # pmi4 = np.array(df2['PMI:在手订单'], dtype=float)
    pmi5 = np.array(df2['PMI:产成品库存'], dtype=float)
    pmi6 = np.array(df2['PMI:采购量'], dtype=float)
    pmi7 = np.array(df2['PMI:进口'], dtype=float)
    pmi8 = np.array(df2['PMI:出厂价格'], dtype=float)
    pmi9 = np.array(df2['PMI:购进价格'], dtype=float)
    pmi10 = np.array(df2['PMI:原材料库存'], dtype=float)
    # pmi11 = np.array(df2['PMI:供货商配送时间'], dtype=float)
    # pmi12 = np.array(df2['PMI:生产经营活动预期'], dtype=float)


    print(correlation(t1, shfe_cu, t2, pmi))
    print(correlation(t1, shfe_cu, t2, pmi1))
    print(correlation(t1, shfe_cu, t2, pmi2))
    print(correlation(t1, shfe_cu, t2, pmi3))
    # print(correlation(t1, shfe_cu, t2, pmi4))
    print(correlation(t1, shfe_cu, t2, pmi5))
    print(correlation(t1, shfe_cu, t2, pmi6))
    print(correlation(t1, shfe_cu, t2, pmi7))
    print(correlation(t1, shfe_cu, t2, pmi8))
    print(correlation(t1, shfe_cu, t2, pmi9))
    print(correlation(t1, shfe_cu, t2, pmi10))

    # return


    t3 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m'))
    us_pmi = np.array(df2['美国:供应管理协会(ISM):PMI:季调'], dtype=float)
    us_pmi1 = np.array(df2['美国:ISM:PMI:新订单:季调'], dtype=float)
    us_pmi2 = np.array(df2['美国:ISM:PMI:产出:季调'], dtype=float)
    us_pmi3 = np.array(df2['美国:ISM:PMI:就业:季调'], dtype=float)
    us_pmi4 = np.array(df2['美国:ISM:PMI:供应商交付:季调'], dtype=float)
    us_pmi5 = np.array(df2['美国:ISM:PMI:自有库存:季调'], dtype=float)
    us_pmi6 = np.array(df2['美国:ISM:PMI:客户库存:季调'], dtype=float)
    us_pmi7 = np.array(df2['美国:ISM:PMI:物价:季调'], dtype=float)
    us_pmi8 = np.array(df2['美国:ISM:PMI:订单库存:季调'], dtype=float)
    us_pmi9 = np.array(df2['美国:ISM:PMI:新出口订单:季调'], dtype=float)
    us_pmi10 = np.array(df2['美国:ISM:PMI:进口:季调'], dtype=float)
    us_pmi11 = np.array(df2['美国:供应管理协会(ISM):服务业PMI:季调'], dtype=float)
    us_pmi12 = np.array(df2['美国:ISM:服务业PMI:商业活动:季调'], dtype=float)
    us_pmi13 = np.array(df2['美国:ISM:服务业PMI:新订单:季调'], dtype=float)
    us_pmi14 = np.array(df2['美国:ISM:服务业PMI:就业:季调'], dtype=float)
    us_pmi15 = np.array(df2['美国:ISM:服务业PMI:供应商交付:季调'], dtype=float)
    us_pmi16 = np.array(df2['美国:ISM:服务业PMI:库存:季调'], dtype=float)
    us_pmi17 = np.array(df2['美国:ISM:服务业PMI:物价:季调'], dtype=float)
    us_pmi18 = np.array(df2['美国:ISM:服务业PMI:订单库存:季调'], dtype=float)
    us_pmi19 = np.array(df2['美国:ISM:服务业PMI:新出口订单:季调'], dtype=float)
    us_pmi20 = np.array(df2['美国:ISM:服务业PMI:进口:季调'], dtype=float)
    us_pmi21 = np.array(df2['美国:ISM:服务业PMI:库存景气:季调'], dtype=float)

    us_pmi22 = np.array(df2['美国:Markit制造业PMI:初值'], dtype=float)
    us_pmi23 = np.array(df2['美国:Markit PMI:服务业:初值'], dtype=float)
    us_pmi24 = np.array(df2['美国:Markit PMI:综合产出:初值'], dtype=float)
    print('US PMI')
    print(correlation(t1, shfe_cu, t3, us_pmi))
    print(correlation(t1, shfe_cu, t3, us_pmi1))
    print(correlation(t1, shfe_cu, t3, us_pmi2))
    print(correlation(t1, shfe_cu, t3, us_pmi3))
    print(correlation(t1, shfe_cu, t3, us_pmi4))
    print(correlation(t1, shfe_cu, t3, us_pmi5))
    print(correlation(t1, shfe_cu, t3, us_pmi6))
    print(correlation(t1, shfe_cu, t3, us_pmi7))
    print(correlation(t1, shfe_cu, t3, us_pmi8))
    print(correlation(t1, shfe_cu, t3, us_pmi9))
    print(correlation(t1, shfe_cu, t3, us_pmi10))
    print(correlation(t1, shfe_cu, t3, us_pmi11))
    print(correlation(t1, shfe_cu, t3, us_pmi12))
    print(correlation(t1, shfe_cu, t3, us_pmi13))
    print(correlation(t1, shfe_cu, t3, us_pmi14))
    print(correlation(t1, shfe_cu, t3, us_pmi15))
    print(correlation(t1, shfe_cu, t3, us_pmi16))
    print(correlation(t1, shfe_cu, t3, us_pmi17))
    print(correlation(t1, shfe_cu, t3, us_pmi18))
    print(correlation(t1, shfe_cu, t3, us_pmi19))
    print(correlation(t1, shfe_cu, t3, us_pmi20))
    print(correlation(t1, shfe_cu, t3, us_pmi21))
    print(correlation(t1, shfe_cu, t3, us_pmi22))
    print(correlation(t1, shfe_cu, t3, us_pmi23))
    print(correlation(t1, shfe_cu, t3, us_pmi24))

    idx = np.logical_not(np.isnan(shfe_cu))
    t11, shfe_cu = get_period_data(t1[idx], shfe_cu[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(pmi))   
    t20, pmi = get_period_data(t2[idx], pmi[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(pmi9))   
    t21, pmi9 = get_period_data(t2[idx], pmi9[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(pmi2))   
    t22, pmi2 = get_period_data(t2[idx], pmi2[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(pmi1))   
    t23, pmi1 = get_period_data(t2[idx], pmi1[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(pmi3))   
    t24, pmi3 = get_period_data(t2[idx], pmi3[idx], start_time, end_time)

    fig1 = figure(frame_width=725, frame_height=170, x_axis_type = "datetime")
    fig1.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig1.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y12'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=45,
            end=54,
        ),
    }
    fig1.line(t20, pmi, color='blue',  y_range_name=y_column2_name, legend_label='PMI')
    fig1.line(t20, 50, color='black', y_range_name=y_column2_name)
    fig1.add_layout(LinearAxis(y_range_name="y12"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 10
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'


    fig11 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig11.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig11.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y112'
    fig11.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(pmi9)*0.9,
            end=np.max(pmi9)*1.1,
        ),
    }
    fig11.line(t21, pmi9, color='blue',  y_range_name=y_column2_name, legend_label='PMI:购进价格')
    fig11.line(t21, 50, color='black', y_range_name=y_column2_name)
    fig11.add_layout(LinearAxis(y_range_name="y112"), 'right')
    fig11.xaxis[0].ticker.desired_num_ticks = 10
    fig11.legend.click_policy="hide"
    fig11.legend.location='top_left'


    fig12 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig12.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig12.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y122'
    fig12.extra_y_ranges = {
        y_column2_name: Range1d(
            start=44,
            end=55,
        ),
    }
    fig12.line(t23, pmi1, color='blue', y_range_name=y_column2_name, legend_label='PMI:生产')
    fig12.line(t23, 50, color='black', y_range_name=y_column2_name)
    fig12.add_layout(LinearAxis(y_range_name="y122"), 'right')
    fig12.xaxis[0].ticker.desired_num_ticks = 10
    fig12.legend.click_policy="hide"
    fig12.legend.location='top_left'


    fig13 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig13.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig13.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y132'
    fig13.extra_y_ranges = {
        y_column2_name: Range1d(
            start=44,
            end=55,
        ),
    }
    fig13.line(t24, pmi3, color='blue', y_range_name=y_column2_name, legend_label='PMI:新出口订单')
    fig13.line(t24, 50, color='black', y_range_name=y_column2_name)
    fig13.add_layout(LinearAxis(y_range_name="y132"), 'right')
    fig13.xaxis[0].ticker.desired_num_ticks = 10
    fig13.legend.click_policy="hide"
    fig13.legend.location='top_left'


    fig14 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig14.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig14.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y22'
    fig14.extra_y_ranges = {
        y_column2_name: Range1d(
            start=40,
            end=np.max(pmi2)*1.1,
        ),
    }
    fig14.line(t22, pmi2, color='blue', y_range_name=y_column2_name, legend_label='PMI:新订单')
    fig14.line(t22, 50, color='black', y_range_name=y_column2_name)
    fig14.add_layout(LinearAxis(y_range_name="y22"), 'right')
    fig14.xaxis[0].ticker.desired_num_ticks = 10
    fig14.legend.click_policy="hide"
    fig14.legend.location='top_left'


    idx = np.logical_not(np.isnan(us_pmi5))   
    t31, us_pmi5 = get_period_data(t3[idx], us_pmi5[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(us_pmi6))   
    t31, us_pmi6 = get_period_data(t3[idx], us_pmi6[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(us_pmi17))   
    t31, us_pmi17 = get_period_data(t3[idx], us_pmi17[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(us_pmi))   
    t32, us_pmi = get_period_data(t3[idx], us_pmi[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(us_pmi21))   
    t33, us_pmi21 = get_period_data(t3[idx], us_pmi21[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(us_pmi22))   
    t34, us_pmi22 = get_period_data(t3[idx], us_pmi22[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(us_pmi24))   
    t35, us_pmi24 = get_period_data(t3[idx], us_pmi24[idx], start_time, end_time)

    fig3 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig3.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig3.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y32'
    fig3.extra_y_ranges = {
        y_column2_name: Range1d(
            # start=np.min(pmi)*0.9,
            # end=np.max(pmi)*1.1,
            start=46,
            end=65,
        ),
    }
    fig3.line(t32, us_pmi, color='blue', y_range_name=y_column2_name, legend_label='美国:供应管理协会(ISM):制造业PMI')
    fig3.add_layout(LinearAxis(y_range_name="y32"), 'right')
    fig3.xaxis[0].ticker.desired_num_ticks = 10
    fig3.legend.click_policy="hide"
    fig3.legend.location='top_left'


    fig31 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig31.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig31.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y312'
    fig31.extra_y_ranges = {
        y_column2_name: Range1d(
            # start=np.min(pmi)*0.9,
            # end=np.max(pmi)*1.1,
            start=45,
            end=90,
        ),
    }
    fig31.line(t31, us_pmi17, color='blue', y_range_name=y_column2_name, legend_label='美国:ISM:非制造业PMI:物价')
    fig31.add_layout(LinearAxis(y_range_name="y312"), 'right')
    fig31.xaxis[0].ticker.desired_num_ticks = 10
    fig31.legend.click_policy="hide"
    fig31.legend.location='top_left'


    fig32 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig32.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig32.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y312'
    fig32.extra_y_ranges = {
        y_column2_name: Range1d(
            # start=np.min(pmi)*0.9,
            # end=np.max(pmi)*1.1,
            start=35,
            end=70,
        ),
    }
    fig32.line(t33, us_pmi21, color='blue', y_range_name=y_column2_name, legend_label='美国:ISM:非制造业PMI:库存景气')
    fig32.add_layout(LinearAxis(y_range_name="y312"), 'right')
    fig32.xaxis[0].ticker.desired_num_ticks = 10
    fig32.legend.click_policy="hide"
    fig32.legend.location='top_left'


    fig33 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig33.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig33.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y332'
    fig33.extra_y_ranges = {
        y_column2_name: Range1d(
            # start=np.min(pmi)*0.9,
            # end=np.max(pmi)*1.1,
            start=35,
            end=70,
        ),
    }
    fig33.line(t34, us_pmi22, color='blue', y_range_name=y_column2_name, legend_label='美国:Markit制造业PMI:初值')
    fig33.add_layout(LinearAxis(y_range_name="y332"), 'right')
    fig33.xaxis[0].ticker.desired_num_ticks = 10
    fig33.legend.click_policy="hide"
    fig33.legend.location='top_left'


    fig34 = figure(frame_width=725, frame_height=170, x_range=fig1.x_range, x_axis_type = "datetime")
    fig34.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig34.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y342'
    fig34.extra_y_ranges = {
        y_column2_name: Range1d(
            # start=np.min(pmi)*0.9,
            # end=np.max(pmi)*1.1,
            start=35,
            end=70,
        ),
    }
    fig34.line(t35, us_pmi24, color='blue', y_range_name=y_column2_name, legend_label='美国:Markit PMI:综合产出:初值')
    fig34.add_layout(LinearAxis(y_range_name="y342"), 'right')
    fig34.xaxis[0].ticker.desired_num_ticks = 10
    fig34.legend.click_policy="hide"
    fig34.legend.location='top_left'

    layout = grid([[fig1, fig3],
                   [fig11, fig31],
                   [fig12, fig32],
                   [fig13, fig14],
                   [fig33, fig34]])

    show(layout)


# 铜 MSCI
def test33():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df1['期货官方价:LME3个月铜'], dtype=float)
    lme_cu_03 = np.array(df1['LME铜升贴水(0-3)'], dtype=float)
    shfe_cu = np.array(df1['期货收盘价(活跃合约):阴极铜'], dtype=float)
    msci_em = np.array(df1['MSCI新兴市场'], dtype=float)
    msci_dm = np.array(df1['MSCI发达市场'], dtype=float)
    msci_us = np.array(df1['MSCI美国'], dtype=float)
    msci_cn = np.array(df1['MSCI中国(美元)'], dtype=float)
    msci_eu = np.array(df1['MSCI欧洲'], dtype=float)

    idx = np.logical_not(np.isnan(shfe_cu))
    t11, shfe_cu = get_period_data(t1[idx], shfe_cu[idx], start_time, end_time)

    idx = np.logical_not(np.isnan(msci_em))   
    t1_em, msci_em = get_period_data(t1[idx], msci_em[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(msci_dm))   
    t1_dm, msci_dm = get_period_data(t1[idx], msci_dm[idx], start_time, end_time)
    
    idx = np.logical_not(np.isnan(msci_us))   
    t1_us, msci_us = get_period_data(t1[idx], msci_us[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(msci_cn))   
    t1_cn, msci_cn = get_period_data(t1[idx], msci_cn[idx], start_time, end_time)
    idx = np.logical_not(np.isnan(msci_eu))   
    t1_eu, msci_eu = get_period_data(t1[idx], msci_eu[idx], start_time, end_time)

    # path2 = os.path.join(data_dir, '汇率'+'.csv') 
    # df2 = pd.read_csv(path2)
    # t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    # usdcny = np.array(df2['即期汇率:美元兑人民币'], dtype=float)

    fig1 = figure(frame_width=725, frame_height=250, x_axis_type = "datetime")
    fig1.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig1.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y12'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(msci_em)*0.9,
            end=np.max(msci_em)*1.1,
        ),
    }
    fig1.line(t1_em, msci_em, color='black', y_range_name=y_column2_name, legend_label='MSCI 新兴市场')
    fig1.add_layout(LinearAxis(y_range_name="y12"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 10
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'


    fig2 = figure(frame_width=725, frame_height=250, x_axis_type = "datetime")
    fig2.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig2.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y12'
    fig2.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(msci_dm)*0.9,
            end=np.max(msci_dm)*1.1,
        ),
    }
    fig2.line(t1_dm, msci_dm, color='black', y_range_name=y_column2_name, legend_label='MSCI 发达市场')
    fig2.add_layout(LinearAxis(y_range_name="y12"), 'right')
    fig2.xaxis[0].ticker.desired_num_ticks = 10
    fig2.legend.click_policy="hide"
    fig2.legend.location='top_left'


    fig3 = figure(frame_width=725, frame_height=250, x_axis_type = "datetime")
    fig3.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig3.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y12'
    fig3.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(msci_us)*0.9,
            end=np.max(msci_us)*1.1,
        ),
    }
    fig3.line(t1_us, msci_us, color='black', y_range_name=y_column2_name, legend_label='MSCI 美国')
    fig3.add_layout(LinearAxis(y_range_name="y12"), 'right')
    fig3.xaxis[0].ticker.desired_num_ticks = 10
    fig3.legend.click_policy="hide"
    fig3.legend.location='top_left'


    fig4 = figure(frame_width=725, frame_height=250, x_axis_type = "datetime")
    fig4.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig4.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y12'
    fig4.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(msci_cn)*0.9,
            end=np.max(msci_cn)*1.1,
        ),
    }
    fig4.line(t1_cn, msci_cn, color='black', y_range_name=y_column2_name, legend_label='MSCI 中国')
    fig4.add_layout(LinearAxis(y_range_name="y12"), 'right')
    fig4.xaxis[0].ticker.desired_num_ticks = 10
    fig4.legend.click_policy="hide"
    fig4.legend.location='top_left'


    fig5 = figure(frame_width=725, frame_height=250, x_axis_type = "datetime")
    fig5.line(t11, shfe_cu, line_width=2, line_color='orange', legend_label='沪铜')
    fig5.y_range = Range1d(np.min(shfe_cu)*0.9, np.max(shfe_cu)*1.1)
    y_column2_name = 'y12'
    fig5.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(msci_eu)*0.9,
            end=np.max(msci_eu)*1.1,
        ),
    }
    fig5.line(t1_eu, msci_eu, color='black', y_range_name=y_column2_name, legend_label='MSCI 欧洲')
    fig5.add_layout(LinearAxis(y_range_name="y12"), 'right')
    fig5.xaxis[0].ticker.desired_num_ticks = 10
    fig5.legend.click_policy="hide"
    fig5.legend.location='top_left'


    layout = grid([[fig1, fig3],
                   [fig2, fig4],
                   [fig5, None]])

    show(layout)

# 精炼铜 保税区库存 和 进口
def test4():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    stock_notax = np.array(df1['库存:铜:上海保税区'], dtype=float)

    idx = np.logical_not(np.isnan(stock_notax))
    t10 = t1[idx]
    stock_notax = stock_notax[idx] * 10000  # 万吨 --> 吨

    import1 = np.array(df1['进口数量:未锻轧铜含量>99.9935%的精炼铜阴极(74031111):当月值'], dtype=float)
    import2 = np.array(df1['进口数量:未锻轧其他精炼铜阴极(74031119):当月值'], dtype=float)
    import3 = np.array(df1['进口数量:未锻轧精炼铜阴极型材(74031190):当月值'], dtype=float)
    import4 = np.array(df1['出口数量:未锻轧的精炼铜线锭(74031200):当月值'], dtype=float)
    import5 = np.array(df1['进口数量:未锻轧的精炼铜坯段(74031300):当月值'], dtype=float)
    import6 = np.array(df1['进口数量:其他未锻轧的精炼铜(74031900):当月值'], dtype=float)

    export1 = np.array(df1['出口数量:未锻轧铜含量>99.9935%的精炼铜阴极(74031111):当月值'], dtype=float)
    export2 = np.array(df1['出口数量:未锻轧其他精炼铜阴极(74031119):当月值'], dtype=float)
    export4 = np.array(df1['出口数量:未锻轧的精炼铜线锭(74031200):当月值'], dtype=float)
    export5 = np.array(df1['出口数量:未锻轧的精炼铜坯段(74031300):当月值'], dtype=float)
    export6 = np.array(df1['出口数量:其他未锻轧的精炼铜(74031900):当月值'], dtype=float)

    t11, import_sum = data_add(t1, import1, t1, import2, replace=0)
    t11, import_sum = data_add(t11, import_sum, t1, import3, replace=0)
    t11, import_sum = data_add(t11, import_sum, t1, import4, replace=0)
    t11, import_sum = data_add(t11, import_sum, t1, import5, replace=0)
    t11, import_sum = data_add(t11, import_sum, t1, import6, replace=0)

    t12, export_sum = data_add(t1, export1, t1, export2, replace=0)
    t12, export_sum = data_add(t12, export_sum, t1, export4, replace=0)
    t12, export_sum = data_add(t12, export_sum, t1, export5, replace=0)
    t12, export_sum = data_add(t12, export_sum, t1, export6, replace=0)
    # 净进口
    t13, net_import = data_sub(t11, import_sum, t12, export_sum, replace=0)
    net_import /= 1000  # 千克 --> 吨
    idx = np.where(net_import > 0)[0]
    t13 = t13[idx]
    net_import = net_import[idx]

    import_sum2 = np.array(df1['进口数量:精炼铜:当月值'], dtype=float)
    idx = np.logical_not(np.isnan(import_sum2))
    t14 = t1[idx]
    import_sum2 = import_sum2[idx]

    import_3 = np.array(df1['进口数量:铜废料及碎料(74040000):当月值'], dtype=float)
    idx = np.logical_not(np.isnan(import_3))
    t15 = t1[idx]
    import_3 = import_3[idx]/1000

    fig1 = figure(frame_width=1400, frame_height=250, x_axis_type = "datetime")
    fig1.line(t10, stock_notax, line_width=2, line_color='orange', legend_label='库存:铜:上海保税区')
    fig1.line(t14, import_sum2, line_width=2, line_color='purple', legend_label='进口数量:精炼铜:当月值')
    fig1.line(t15, import_3, line_width=2, line_color='darkgreen', legend_label='进口数量:废铜:当月值')
    fig1.line(t13, net_import, color='blue', legend_label='精炼铜 净进口')
    # fig1.y_range = Range1d(np.min(stock_notax)*0.9, np.max(stock_notax)*1.1)
    # y_column2_name = 'y2'
    # fig1.extra_y_ranges = {
    #     y_column2_name: Range1d(
    #         start=np.min(net_import)*0.9,
    #         end=np.max(net_import)*1.1,
    #     ),
    # }
    # fig1.line(t13, net_import, color='blue',  y_range_name=y_column2_name, legend_label='精炼铜 净进口')
    # fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    cu_cny_spot = np.array(df1['长江有色市场:平均价:铜:1#'], dtype=float)
    idx = np.logical_not(np.isnan(cu_cny_spot))
    t17 = t1[idx]
    cu_cny_spot = cu_cny_spot[idx]

    lme3 = np.array(df1['期货官方价:LME3个月铜'], dtype=float)
    lme03 = np.array(df1['LME铜升贴水(0-3)'], dtype=float)
    lme_spot = np.array(df1['现货结算价:LME铜'], dtype=float)

    path2 = os.path.join(data_dir, '汇率'+'.csv') 
    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    usdcny = np.array(df2['即期汇率:美元兑人民币'], dtype=float)
    idx = np.logical_not(np.isnan(lme_spot))
    t15 = t1[idx]
    lme_spot = lme_spot[idx]
    t16, lme0 = data_add(t1, lme3, t1, lme03)

    tmp = lme_spot + 100
    t21, cu_cny_import = data_mul(t15, tmp, t2, usdcny)
    cu_cny_import *= 1.17
    cu_cny_import += 200

    t22, import_profit = data_sub(t17, cu_cny_spot, t21, cu_cny_import)

    fig2 = figure(frame_width=1400, frame_height=250, x_range=fig1.x_range, x_axis_type = "datetime")
    # fig2.line(t21, cu_cny_import, line_width=2, line_color='orange', legend_label='铜 进口价 估计')
    # fig2.line(t17, cu_cny_spot, line_width=2, line_color='blue', legend_label='铜 国内现货价')
    
    fig2.line(t22, import_profit, line_width=2, line_color='orange', legend_label='进口利润(估计)')
    show(column(fig1,fig2))

    # _, diff = data_sub(t15, lme_spot, t16, lme0) 
    # print(lme03[-50:])
    # print(diff[-50:])


# 现货升贴水
def test5():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df1['期货收盘价:LME3个月铜'], dtype=float)
    shfe_cu = np.array(df1['期货收盘价(主力):阴极铜'], dtype=float)

    path2 = os.path.join(data_dir, '汇率'+'.csv') 
    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    usdcny = np.array(df2['即期汇率:美元兑人民币'], dtype=float)

    lme03 = np.array(df1['LME铜升贴水(0-3)'], dtype=float)
    idx = np.logical_not(np.isnan(lme03))
    t10 = t1[idx]
    lme03 = lme03[idx]
    t10, lme03_cny = data_mul(t10, lme03, t2, usdcny)

    premium31 = np.array(df1['上海有色市场:洋山铜溢价:平均价'], dtype=float)
    idx = np.logical_not(np.isnan(premium31))
    t13 = t1[idx]
    premium31 = premium31[idx]
    t13, premium31 = data_mul(t13, premium31, t2, usdcny)


    idx = np.logical_not(np.isnan(shfe_cu))
    fig1 = figure(frame_width=1400, frame_height=320, x_axis_type = "datetime")
    fig1.y_range = Range1d(np.min(shfe_cu[idx])*0.9, np.max(shfe_cu[idx])*1.1)
    fig1.line(t1[idx], shfe_cu[idx], line_width=2, line_color='orange', legend_label='期货收盘价(主力):阴极铜')
    fig1.xaxis[0].ticker.desired_num_ticks = 20

    idx = np.logical_not(np.isnan(lme_cu_3m))
    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(lme_cu_3m[idx])*0.9,
            end=np.max(lme_cu_3m[idx])*1.1,
        ),
    }
    fig1.line(t1[idx], lme_cu_3m[idx], color='blue',  y_range_name=y_column2_name, legend_label='LME 3个月铜 美元 右轴')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')
    fig1.legend.location='top_left'

    fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime")
    fig2.line(t10, lme03_cny, line_width=2, line_color='black', legend_label='LME铜升贴水(0-3) 人民币')
    fig2.line(t13, premium31, line_width=2, line_color='chartreuse', legend_label='上海有色市场:洋山铜溢价:平均价')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"
    fig2.legend.location='top_left'

    plot_seasonality(t13, premium31, title='洋山铜溢价')

    # cu_c1 = np.array(df1['期货收盘价:铜:连一'], dtype=float)
    # cu_c3 = np.array(df1['期货收盘价:铜:连三'], dtype=float)
    # t14, cu_diff = data_sub(t1, cu_c1, t1, cu_c3)
    # fig2 = figure(frame_width=1400, frame_height=300, x_range=fig1.x_range, x_axis_type = "datetime")
    # fig2.line(t14, cu_diff, line_width=2, line_color='black', legend_label='cu c1-c3 (左)')
    # fig2.y_range = Range1d(np.min(cu_diff)*0.9, np.max(cu_diff)*1.1)

    # idx = np.logical_not(np.isnan(cu_c1))
    # t15 = t1[idx]
    # cu_c1 = cu_c1[idx]
    # idx = np.logical_not(np.isnan(cu_c3))
    # t16 = t1[idx]
    # cu_c3 = cu_c3[idx]
    # y_column2_name = 'y2'
    # fig2.extra_y_ranges = {
    #     y_column2_name: Range1d(
    #         start=np.min(cu_c1)*0.9,
    #         end=np.max(cu_c1)*1.1,
    #     ),
    # }

    # fig2.line(t15, cu_c1, color='orange',  y_range_name=y_column2_name, legend_label='cu c1 (右)')
    # fig2.line(t16, cu_c3, color='blue',  y_range_name=y_column2_name, legend_label='cu c3 (右)')
    # fig2.add_layout(LinearAxis(y_range_name="y2"), 'right')
    # fig2.xaxis[0].ticker.desired_num_ticks = 20
    # fig2.legend.click_policy="hide"
    # fig2.legend.location='top_left'

    show(column(fig1,fig2))


# TC/RC
def test6():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))

    tcrc = np.array(df1['长江有色市场:铜精矿TC/RC:平均价'], dtype=float)
    idx = np.logical_not(np.isnan(tcrc))
    t10 = t1[idx]
    tcrc = tcrc[idx]

    cu_c1 = np.array(df1['期货收盘价(主力):阴极铜'], dtype=float)
    idx = np.logical_not(np.isnan(cu_c1))
    t11 = t1[idx]
    cu_c1 = cu_c1[idx]


    fig1 = figure(frame_width=1400, frame_height=300, x_axis_type = "datetime")
    fig1.line(t10, tcrc, line_width=2, line_color='orange', legend_label='现货:中国铜冶炼厂:TCRC (左)')
    fig1.y_range = Range1d(np.min(tcrc)*0.9, np.max(tcrc)*1.1)
    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(cu_c1)*0.9,
            end=np.max(cu_c1)*1.1,
        ),
    }
    fig1.line(t11, cu_c1, line_width=2, color='black', y_range_name=y_column2_name, legend_label='cu c1 (右)')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')

    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    show(fig1)


def test66():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))

    production_air = np.array(df1['产量:空调:当月值'], dtype=float)
    production_elec = np.array(df1['产量:发电设备:当月值'], dtype=float)
    production_wire = np.array(df1['产量:光缆:当月值'], dtype=float)
    production_frig = np.array(df1['产量:家用电冰箱:当月值'], dtype=float)
    production_li = np.array(df1['产量:锂离子电池:当月值'], dtype=float)
    production_car = np.array(df1['产量:新能源汽车:当月值'], dtype=float)
    production_house = np.array(df1['房屋新开工面积:累计值'], dtype=float)

    t11, production_air = get_period_data(t1, production_air, start_time, end_time, remove_nan=True)
    t12, production_elec = get_period_data(t1, production_elec, start_time, end_time, remove_nan=True)
    t13, production_wire = get_period_data(t1, production_wire, start_time, end_time, remove_nan=True)
    t14, production_frig = get_period_data(t1, production_frig, start_time, end_time, remove_nan=True)
    t15, production_li = get_period_data(t1, production_li, start_time, end_time, remove_nan=True)
    t16, production_car = get_period_data(t1, production_car, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=725, frame_height=250, x_axis_type = "datetime")
    fig1.line(t11, production_air, line_width=2, line_color='orange', legend_label='产量:空调:当月值')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    fig2 = figure(frame_width=725, frame_height=250, x_range=fig1.x_range, x_axis_type = "datetime")
    fig2.line(t12, production_elec, line_width=2, line_color='orange', legend_label='产量:发电设备:当月值')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"
    fig2.legend.location='top_left'

    fig3 = figure(frame_width=725, frame_height=250, x_range=fig1.x_range, x_axis_type = "datetime")
    fig3.line(t13, production_wire, line_width=2, line_color='orange', legend_label='产量:光缆:当月值')
    fig3.xaxis[0].ticker.desired_num_ticks = 20
    fig3.legend.click_policy="hide"
    fig3.legend.location='top_left'

    fig4 = figure(frame_width=725, frame_height=250, x_range=fig1.x_range, x_axis_type = "datetime")
    fig4.line(t14, production_frig, line_width=2, line_color='orange', legend_label='产量:家用电冰箱:当月值')
    fig4.xaxis[0].ticker.desired_num_ticks = 20
    fig4.legend.click_policy="hide"
    fig4.legend.location='top_left'

    fig5 = figure(frame_width=725, frame_height=250, x_range=fig1.x_range, x_axis_type = "datetime")
    fig5.line(t15, production_li, line_width=2, line_color='orange', legend_label='产量:锂离子电池:当月值')
    fig5.xaxis[0].ticker.desired_num_ticks = 20
    fig5.legend.click_policy="hide"
    fig5.legend.location='top_left'

    fig6 = figure(frame_width=725, frame_height=250, x_range=fig1.x_range, x_axis_type = "datetime")
    fig6.line(t16, production_car, line_width=2, line_color='orange', legend_label='产量:新能源汽车:当月值')
    fig6.xaxis[0].ticker.desired_num_ticks = 20
    fig6.legend.click_policy="hide"
    fig6.legend.location='top_left'


    layout = grid([[fig1, fig3],
                   [fig2, fig4],
                   [fig5, fig6]])

    show(layout)


# 铜期货价格与经济数据
def test7():
    path1 = os.path.join(data_dir, '铜'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    # car = np.array(df1['产量:新能源汽车:当月值'], dtype=float)
    p1 = np.array(df1['价格:废铜:1#光亮铜线:上海'], dtype=float) 
    p2 = np.array(df1['长江有色市场:平均价:铜:1#'], dtype=float) 
    # 精废铜价差
    t11, diff1 = data_sub(t1, p2, t1, p1)

    cu_c1 = np.array(df1['期货收盘价(主力):阴极铜'], dtype=float)
    idx = np.logical_not(np.isnan(cu_c1))
    t12 = t1[idx]
    cu_c1 = cu_c1[idx]


    path2 = os.path.join(data_dir, '铜期货价格'+'.csv') 
    df2 = pd.read_csv(path2).dropna()
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    cu01_open = np.array(df2['期货开盘价:铜:连一'], dtype=float)
    cu02_open = np.array(df2['期货开盘价:铜:连二'], dtype=float)
    cu03_open = np.array(df2['期货开盘价:铜:连三'], dtype=float)
    cu04_open = np.array(df2['期货开盘价:铜:连四'], dtype=float)
    cu05_open = np.array(df2['期货开盘价:铜:连五'], dtype=float)
    cu06_open = np.array(df2['期货开盘价:铜:连六'], dtype=float)
    cu01_close = np.array(df2['期货收盘价:铜:连一'], dtype=float)
    cu02_close = np.array(df2['期货收盘价:铜:连二'], dtype=float)
    cu03_close = np.array(df2['期货收盘价:铜:连三'], dtype=float)
    cu04_close = np.array(df2['期货收盘价:铜:连四'], dtype=float)
    cu05_close = np.array(df2['期货收盘价:铜:连五'], dtype=float)
    cu06_close = np.array(df2['期货收盘价:铜:连六'], dtype=float)

    diff2 = cu01_close - cu03_close


    fig1 = figure(frame_width=1400, frame_height=300, x_axis_type = "datetime")

    fig1.line(t11, diff1, line_width=2, line_color='purple', legend_label='精废铜价差')
    fig1.y_range = Range1d(np.min(diff1)*1.1, np.max(diff1)*1.1)
    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(diff2)*0.9,
            end=np.max(diff2)*1.1,
        ),
    }

    fig1.line(t2, diff2, color='blue', y_range_name=y_column2_name, legend_label='cu c1-c3 (右)')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')

    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left'

    show(fig1)


# 交割日前后的期限结构
def test8():
    path1 = os.path.join(data_dir, '铜期货价格'+'.csv') 
    df1 = pd.read_csv(path1).dropna()
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))

    # cu01_close = np.array(df1['期货收盘价:铜:连一'], dtype=float)
    # cu02_close = np.array(df1['期货收盘价:铜:连二'], dtype=float)
    # cu03_close = np.array(df1['期货收盘价:铜:连三'], dtype=float)
    # cu04_close = np.array(df1['期货收盘价:铜:连四'], dtype=float)
    # cu05_close = np.array(df1['期货收盘价:铜:连五'], dtype=float)
    # cu06_close = np.array(df1['期货收盘价:铜:连六'], dtype=float)
    # cu07_close = np.array(df1['期货收盘价:铜:连七'], dtype=float)
    # cu08_close = np.array(df1['期货收盘价:铜:连八'], dtype=float)
    # cu09_close = np.array(df1['期货收盘价:铜:连九'], dtype=float)
    # cu10_close = np.array(df1['期货收盘价:铜:连十'], dtype=float)
    # cu11_close = np.array(df1['期货收盘价:铜:连十一'], dtype=float)
    # cu12_close = np.array(df1['期货收盘价:铜:连十二'], dtype=float)
    # plot_term_structure(t1, 
    # [cu01_close,cu02_close,cu03_close,cu04_close,cu05_close,cu06_close,
    #  cu07_close,cu08_close,cu09_close,cu10_close,cu11_close,cu12_close],
    # ['2022-10-13','2022-10-14','2022-10-17','2022-10-18','2022-10-19','2022-10-20','2022-10-21','2022-10-24'],
    # '2023-5-31')

    cu_close = np.array(df1[['期货收盘价:铜:1月份合约','期货收盘价:铜:2月份合约','期货收盘价:铜:3月份合约','期货收盘价:铜:4月份合约',
                            '期货收盘价:铜:5月份合约','期货收盘价:铜:6月份合约','期货收盘价:铜:7月份合约','期货收盘价:铜:8月份合约',
                            '期货收盘价:铜:9月份合约','期货收盘价:铜:10月份合约','期货收盘价:铜:11月份合约','期货收盘价:铜:12月份合约']], dtype=float)

    L = len(t1)
    # 最后交易日前后各几天
    days_around = 5
    # 取之后几个月的合约价格
    months_after = 6
    today_15 = datetime.datetime.strptime('2002-02-15', '%Y-%m-%d')

    time_df = pd.DataFrame()
    new_data = np.empty([0, months_after],dtype=float)
    while (1):
        days_num = calendar.monthrange(today_15.year, today_15.month)[1]
        today_15 = today_15 + pd.Timedelta(days = days_num)
        today_tmp = today_15

        if (today_15 >= datetime.datetime.strptime('2022-11-01', '%Y-%m-%d')):
            break

        # 节假日顺延，找15号之后第一个有数据的一天
        while (1):
            where = np.where(t1 == today_tmp)[0]
            # 15号有数据
            if (len(where) == 0):
                today_tmp += pd.Timedelta(days = 1)
            else:
                break

        idx = np.where(t1 == today_tmp)[0][0]
        time_df = pd.concat([time_df, df1['time'][idx-days_around:idx+days_around+1]], axis=0)
        new_data = np.vstack([new_data, cu_close[idx-days_around:idx+days_around+1, [x%12 for x in range(today_15.month,today_15.month+months_after)]]])
    
    time_df.columns=['time']
    new_time = pd.DatetimeIndex(pd.to_datetime(time_df['time'], format='%Y-%m-%d'))
    print(new_time)
    print(new_data)

    avg_time = new_time[days_around::(days_around*2+1)]
    n = int(len(new_data)/(2*days_around+1))
    avg_before = np.empty((n), dtype=float)
    avg_after = np.empty((n), dtype=float)
    avg_diff = np.empty((n), dtype=float)
    for i in range(n):
        # 价差
        avg_before[i] = np.average(new_data[days_around+i*(days_around*2+1)-2:days_around+i*(days_around*2+1)+1, 1]-new_data[days_around+i*(days_around*2+1)-2:days_around+i*(days_around*2+1)+1, 4])
        avg_after[i] = np.average(new_data[days_around+i*(days_around*2+1)+1:days_around+i*(days_around*2+1)+3, 1]-new_data[days_around+i*(days_around*2+1)+1:days_around+i*(days_around*2+1)+3, 4])
        # 绝对价格
        # avg_before[i] = np.average(new_data[days_around+i*(days_around*2+1)-1:days_around+i*(days_around*2+1)+1, 1])
        # avg_after[i] = np.average(new_data[days_around+i*(days_around*2+1)+2:days_around+i*(days_around*2+1)+5, 1])

    avg_diff = avg_after - avg_before

    path2 = os.path.join(data_dir, '铜'+'.csv') 
    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    stock_notax = np.array(df2['库存:铜:上海保税区'], dtype=float) 
    lme_stock = np.array(df2['总库存:LME铜'], dtype=float)
    sh_stock = np.array(df2['库存期货:阴极铜'], dtype=float)
    stock = sh_stock
    t2_diff = np.empty((n), dtype=pd.Timestamp)
    stock_diff = np.zeros((n), dtype=float)
    for i in range(n):
        where = np.where(t2 == avg_time[i])[0]
        if (len(where) == 1):
            idx = where[0]
            if (not np.isnan(stock[idx])):
                t2_diff[i] = t2[idx]
                stock_diff[i] = (stock[idx] + stock[idx-1] - stock[idx-2] - stock[idx-3])/stock[idx-3]

    print(stock_diff)
    idx = np.logical_not(np.isnan(stock_diff))
    t2_diff = t2_diff[idx]
    stock_diff = stock_diff[idx]

    avg_diff /= np.max(avg_diff)
    stock_diff /= np.max(stock_diff)

    fig1 = figure(frame_width=1400, frame_height=600, x_axis_type = "datetime")
    fig1.vbar(x=t2_diff-pd.Timedelta(days = 4), top=stock_diff, line_width=2, color='orange', legend_label='库存差')
    fig1.vbar(x=avg_time, top=avg_diff, line_width=2, color='purple', legend_label='价差')

    # fig1.y_range = Range1d(np.min(avg_diff)*1.1, np.max(avg_diff)*1.1)

    # y_column2_name = 'y2'
    # fig1.extra_y_ranges = {
    #     y_column2_name: Range1d(
    #         start=np.min(sh_stock_diff)*1.1,
    #         end=np.max(sh_stock_diff)*1.1,
    #     ),
    # }
    # fig1.vbar(x=t2_diff+pd.Timedelta(days = 4), top=sh_stock_diff, y_range_name=y_column2_name, line_width=2, color='orange', legend_label='库存差')
    # fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')

    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig1.legend.location='top_left' 
    show(fig1)

# cu01-02 和 bc01-02 的关系
def test9():
    path1 = os.path.join(data_dir, '沪铜连一'+'.csv') 
    df1 = pd.read_csv(path1).dropna()
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    cu01_close = np.array(df1['收盘价(元)'], dtype=float) 
    t1, cu01_close = get_period_data(t1, cu01_close, '2022-07-20 09:45:00', '2022-10-27 10:35:00', format='%Y-%m-%d %H:%M:%S')

    path2 = os.path.join(data_dir, '沪铜连二'+'.csv') 
    df2 = pd.read_csv(path2).dropna()
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    cu02_close = np.array(df2['收盘价(元)'], dtype=float) 
    t2, cu02_close = get_period_data(t2, cu02_close, '2022-07-20 09:45:00', '2022-10-27 10:35:00', format='%Y-%m-%d %H:%M:%S')

    path3 = os.path.join(data_dir, '国际铜连一'+'.csv') 
    df3 = pd.read_csv(path3).dropna()
    t3 = pd.DatetimeIndex(pd.to_datetime(df3['time'], format='%Y-%m-%d'))
    bc01_close = np.array(df3['收盘价(元)'], dtype=float) 
    t3, bc01_close = get_period_data(t3, bc01_close, '2022-07-20 09:45:00', '2022-10-27 10:35:00', format='%Y-%m-%d %H:%M:%S')

    path4 = os.path.join(data_dir, '国际铜连二'+'.csv') 
    df4 = pd.read_csv(path4).dropna()
    t4 = pd.DatetimeIndex(pd.to_datetime(df4['time'], format='%Y-%m-%d'))
    bc02_close = np.array(df4['收盘价(元)'], dtype=float) 
    t4, bc02_close = get_period_data(t4, bc02_close, '2022-07-20 09:45:00', '2022-10-27 10:35:00', format='%Y-%m-%d %H:%M:%S')

    # 时间对齐
    idx1 = np.isin(t1, t2)
    idx2 = np.isin(t2, t1)
    t1 = t1[idx1]
    # t2 = t2[idx2]
    cu01_close = cu01_close[idx1]
    cu02_close = cu02_close[idx2]

    idx1 = np.isin(t1, t3)
    idx2 = np.isin(t3, t1)
    t1 = t1[idx1]
    # t3 = t3[idx2]
    cu01_close = cu01_close[idx1]
    cu02_close = cu02_close[idx1]
    bc01_close = bc01_close[idx2]

    idx1 = np.isin(t1, t4)
    idx2 = np.isin(t4, t1)
    t1 = t1[idx1]
    # t3 = t3[idx2]
    cu01_close = cu01_close[idx1]
    cu02_close = cu02_close[idx1]
    bc01_close = bc01_close[idx1]
    bc02_close = bc02_close[idx2]

    print(len(cu01_close),len(cu02_close),len(bc01_close),len(bc02_close),len(t1))

    cu_diff = cu01_close - cu02_close
    bc_diff = bc01_close - bc02_close
    diff = cu_diff - bc_diff
    fig1 = figure(frame_width=1500, frame_height=400, x_axis_type = "datetime")
    # fig1.line(t1, diff, line_width=2, line_color='orange', legend_label='diff')
    # fig1.circle(x=cu_diff, y=bc_diff, color="blue")

    slope, intercept, _, _, _ = linregress(cu_diff, bc_diff)
    yy = cu_diff * slope + intercept
    # fig1.line(x=cu_diff, y=yy, color="black")
    fig1.line(t1, cu_diff, line_width=2, color='orange', legend_label='cu01 - cu02')
    fig1.line(t1, bc_diff, line_width=2, color='blue', legend_label='bc01 - bc02')
    # fig1.vbar(x=t1, top=cu_diff, line_width=2, color='orange', legend_label='cu01 - cu02')
    # fig1.vbar(x=t1, top=bc_diff, line_width=2, color='blue', legend_label='bc01 - bc02')

    fig2 = figure(frame_width=1500, frame_height=500, x_range=fig1.x_range, x_axis_type = "datetime")
    fig2.line(t1, diff, line_width=2, line_color='black', legend_label='(cu01 - cu02) - (bc01 - bc02)')
    show(column(fig1,fig2))

    print(linregress(cu_diff, bc_diff))
    pass

# cu价差 和 bc价差 的价差
def test10():
    cu_list = ['cu2109','cu2110','cu2111','cu2112','cu2201','cu2202','cu2203','cu2204','cu2205','cu2206','cu2207','cu2208','cu2209','cu2210','cu2211','cu2212','cu2301','cu2302','cu2303']
    bc_list = ['bc2109','bc2110','bc2111','bc2112','bc2201','bc2202','bc2203','bc2204','bc2205','bc2206','bc2207','bc2208','bc2209','bc2210','bc2211','bc2212','bc2301','bc2302','bc2303']
    list_len = len(cu_list)
    
    cu_df_list = []
    bc_df_list = []
    for i in range(list_len):
        # 读cu数据
        path = os.path.join(data_dir, cu_list[i]+'_1min'+'.csv') 
        cu_df_list.append(pd.read_csv(path).dropna())
        # 读bc数据
        path = os.path.join(data_dir, bc_list[i]+'_1min'+'.csv') 
        bc_df_list.append(pd.read_csv(path).dropna())

    t = np.empty([0],dtype=np.datetime64)
    cu_price_c1 = np.empty([0],dtype=float)
    cu_price_c2 = np.empty([0],dtype=float)
    bc_price_c1 = np.empty([0],dtype=float)
    bc_price_c2 = np.empty([0],dtype=float)

    cu_n1 = 0
    cu_n2 = 1
    bc_n1 = 0
    bc_n2 = 1

    fig1 = figure(frame_width=1400, frame_height=400, x_axis_type = "datetime", title='x月5日 至 x+1月25日, x+2,x+3合约')
    fig2 = figure(frame_width=1400, frame_height=400, x_axis_type = "datetime", x_range=fig1.x_range)
    for i in range(3,list_len-2):
        start_time = datetime.datetime.strptime('20'+cu_list[i+cu_n1][2:], '%Y%m')
        # x-3月1日 ~ x-1月1日
        # end_time = datetime.datetime(start_time.year, start_time.month, 1) - pd.Timedelta(days=3)
        # end_time = datetime.datetime(end_time.year, end_time.month, 1)
        # start_time = datetime.datetime(start_time.year, start_time.month, 1) - pd.Timedelta(days=63)
        # start_time = datetime.datetime(start_time.year, start_time.month, 1)

        # x-2月1日 ~ x-1月20日
        end_time = datetime.datetime(start_time.year, start_time.month, 1) - pd.Timedelta(days=3)
        end_time = datetime.datetime(end_time.year, end_time.month, 20)
        start_time = datetime.datetime(start_time.year, start_time.month, 1) - pd.Timedelta(days=33)
        start_time = datetime.datetime(start_time.year, start_time.month, 1)

        while (1):
            if (is_workday(start_time)):
                break
            start_time = start_time + pd.Timedelta(days=1)
        start_time = start_time + pd.Timedelta(days=1)
        print(start_time, '--', end_time, '合约:', cu_list[i+cu_n1][2:], cu_list[i+cu_n2][2:])

        # 读CU在start_time和end_time之间的数据
        cu_t1 = pd.DatetimeIndex(pd.to_datetime(cu_df_list[i+cu_n1]['time'], format='%Y-%m-%d'))
        cu_t2 = pd.DatetimeIndex(pd.to_datetime(cu_df_list[i+cu_n2]['time'], format='%Y-%m-%d'))
        cu_price1 = np.array(cu_df_list[i+cu_n1]['收盘价(元)'], dtype=float)
        cu_price2 = np.array(cu_df_list[i+cu_n2]['收盘价(元)'], dtype=float)
        idx1 = np.where((start_time <= cu_t1) & (cu_t1 <= end_time))
        cu_t1 = cu_t1[idx1]
        cu_price1 = cu_price1[idx1]
        idx2 = np.where((start_time <= cu_t2) & (cu_t2 <= end_time))
        cu_t2 = cu_t2[idx2]    
        cu_price2 = cu_price2[idx2]
        idx1 = np.isin(cu_t1, cu_t2)
        idx2 = np.isin(cu_t2, cu_t1)
        cu_t1 = cu_t1[idx1]
        cu_price1 = cu_price1[idx1]
        cu_price2 = cu_price2[idx2]
        print(len(cu_price1), len(cu_price2))

        # 读BC在start_time和end_time之间的数据
        bc_df_list[i+bc_n1].drop_duplicates('time', keep='last', inplace=True)
        bc_df_list[i+bc_n2].drop_duplicates('time', keep='last', inplace=True)
        bc_t1 = pd.DatetimeIndex(pd.to_datetime(bc_df_list[i+bc_n1]['time'], format='%Y-%m-%d'))
        bc_t2 = pd.DatetimeIndex(pd.to_datetime(bc_df_list[i+bc_n2]['time'], format='%Y-%m-%d'))
        bc_price1 = np.array(bc_df_list[i+bc_n1]['收盘价(元)'], dtype=float)
        bc_price2 = np.array(bc_df_list[i+bc_n2]['收盘价(元)'], dtype=float)
        idx1 = np.where((start_time <= bc_t1) & (bc_t1 <= end_time))
        bc_t1 = bc_t1[idx1]
        bc_price1 = bc_price1[idx1]
        idx2 = np.where((start_time <= bc_t2) & (bc_t2 <= end_time))
        bc_t2 = bc_t2[idx2]    
        bc_price2 = bc_price2[idx2]
        idx1 = np.isin(bc_t1, bc_t2)
        idx2 = np.isin(bc_t2, bc_t1)
        bc_t1 = bc_t1[idx1]
        bc_price1 = bc_price1[idx1]
        bc_price2 = bc_price2[idx2]
        print(len(bc_price1), len(bc_price2))


        cu_idx = np.isin(cu_t1, bc_t1)
        bc_idx = np.isin(bc_t1, cu_t1)
        cu_t1 = cu_t1[cu_idx]
        cu_price1 = cu_price1[cu_idx]
        cu_price2 = cu_price2[cu_idx]
        bc_price1 = bc_price1[bc_idx]
        bc_price2 = bc_price2[bc_idx]

        idx = np.empty((len(cu_t1)), dtype=bool)
        for k in range(len(cu_t1)):
            ts = pd.Timestamp(cu_t1[k])
            hour = ts.hour
            minute = ts.minute
            if ((hour == 0 and minute >= 58) or (hour == 14 and minute >= 58) or 
                (hour == 9 and minute <= 0)  or (hour == 21 and minute <= 0) or (hour == 11 and minute >= 28)):
                idx[k] = False
            else:
                idx[k] = True
        cu_t1 = cu_t1[idx]
        cu_price1 = cu_price1[idx]
        cu_price2 = cu_price2[idx]
        bc_price1 = bc_price1[idx]
        bc_price2 = bc_price2[idx]
        if (i%2==0):
            fig1.line(cu_t1, (cu_price1-cu_price2)-(bc_price1-bc_price2), line_width=2, line_color=many_colors[i%2+1])
        else:
            fig2.line(cu_t1, (cu_price1-cu_price2)-(bc_price1-bc_price2), line_width=2, line_color=many_colors[i%2+1])
        t = np.concatenate([t, cu_t1])
        cu_price_c1 = np.concatenate([cu_price_c1, cu_price1])
        cu_price_c2 = np.concatenate([cu_price_c2, cu_price2])
        bc_price_c1 = np.concatenate([bc_price_c1, bc_price1])
        bc_price_c2 = np.concatenate([bc_price_c2, bc_price2])
    
    # fig1 = figure(frame_width=1400, frame_height=600, x_axis_type = "datetime", title='x月28日 - x+1月28日, x+2,x+3合约')

    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"
    show(column(fig1,fig2))

    cu_diff = cu_price_c1 - cu_price_c2
    bc_diff = bc_price_c1 - bc_price_c2
    slope, intercept, r, _, _ = linregress(cu_diff, bc_diff)
    print(linregress(cu_diff, bc_diff))
    fig3 = figure(frame_width=900, frame_height=900)
    fig3.circle(x=cu_diff, y=bc_diff, color="purple", legend='x = 沪铜价差, y = 国际铜价差, y = 0.917*x - 89, r^2 = ' + str(r*r))
    yy = cu_diff * slope + intercept
    fig3.line(x=cu_diff, y=yy, color="black")
    fig3.legend.location='top_left' 
    # show(fig3)
    pass


# 铜金比 和 利率
def test12():
    path = os.path.join(data_dir, '利率'+'.csv') 
    df = pd.read_csv(path)
    t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cn02y = np.array(df['中国国债收益率2年'], dtype=float)
    cn10y = np.array(df['中国国债收益率10年'], dtype=float)
    cn30y = np.array(df['中国国债收益率30年'], dtype=float)
    t01, cn02y = get_period_data(t0, cn02y, start_time, end_time)
    t02, cn10y = get_period_data(t0, cn10y, start_time, end_time)
    t03, cn30y = get_period_data(t0, cn30y, start_time, end_time)

    path1 = os.path.join(data_dir, '金银'+'.csv') 
    df1 = pd.read_csv(path1)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    gold_price_usd = np.array(df1['期货收盘价:COMEX黄金'], dtype=float)
    gold_price_cny = np.array(df1['期货收盘价(主力):黄金'], dtype=float)

    path2 = os.path.join(data_dir, '铜'+'.csv') 
    df2 = pd.read_csv(path2)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df2['期货收盘价:LME3个月铜'], dtype=float)
    shfe_cu = np.array(df2['期货收盘价(主力):阴极铜'], dtype=float)
    
    t3, cu_au1 = data_div(t2, lme_cu_3m, t1, gold_price_usd)
    t4, cu_au2 = data_div(t2, shfe_cu/1.13, t1, gold_price_cny*31.103481)

    fig1 = figure(frame_width=1800, frame_height=300, tools=TOOLS, x_axis_type = "datetime")
    fig1.line(t3, cu_au1, line_width=2, line_color='gray', legend_label='铜金比usd')
    fig1.line(t4, cu_au2, line_width=2, line_color='black', legend_label='铜金比cny')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    fig2 = figure(frame_width=1800, frame_height=300, tools=TOOLS, x_range=fig1.x_range, x_axis_type = "datetime", y_axis_location="right") 
    fig2.line(t01, cn02y/100, line_width=2, line_color='orange', legend_label='中国国债收益率2年')
    fig2.line(t02, cn10y/100, line_width=2, line_color='blue', legend_label='中国国债收益率:10年')
    fig2.yaxis[0].formatter = NumeralTickFormatter(format='0.0%')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    show(column(fig1,fig2))

# LME 0-3
def test13():
    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    lme_cu_03 = np.array(df['LME铜升贴水(0-3)'], dtype=float)
    t00, lme_cu_3m = get_period_data(t, lme_cu_3m, start_time, end_time, remove_nan=True)
    t01, lme_cu_03 = get_period_data(t, lme_cu_03, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1800, frame_height=300, x_axis_type = "datetime")
    fig1.line(t01, lme_cu_3m, line_width=2, line_color='black', legend_label='期货收盘价:LME3个月铜')
    fig1.y_range = Range1d(np.min(lme_cu_3m)*0.9, np.max(lme_cu_3m)*1.1)
    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(lme_cu_03)*1.1,
            end=np.max(lme_cu_03)*1.1,
        ),
    }
    fig1.line(t01, lme_cu_03, tools=TOOLS, line_width=2, color='blue', y_range_name=y_column2_name, legend_label='LME铜升贴水(0-3)')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    show(fig1)

# 澳元
def test14():
    start_time = '2005-1-1'
    end_time = '2029-12-31'

    path = os.path.join(data_dir, '汇率'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    audusd = np.array(df['澳大利亚元兑美元'], dtype=float)

    t3, audusd = get_period_data(t, audusd, start_time, end_time, remove_nan=True)
    idx = np.where(audusd > 5)[0][0]
    audusd[idx] = audusd[idx-1]

    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    t, lme_cu_3m = get_period_data(t, lme_cu_3m, start_time, end_time, remove_nan=True)

    cftc_plot_financial(t3, audusd, 'CME AUDUSD', t, lme_cu_3m, 'LME3个月铜', code='232741', inst_name='CME:AUDUSD')

# 人民币
def test15():
    start_time = '2016-1-1'
    end_time = '2028-12-31'

    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    t00, lme_cu_3m = get_period_data(t, lme_cu_3m, start_time, end_time, remove_nan=True)

    path = os.path.join(data_dir, '汇率'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    usdcny = np.array(df['即期汇率:美元兑人民币'], dtype=float)
    t01, usdcny = get_period_data(t, usdcny, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1800, frame_height=300, x_axis_type = "datetime", tools=TOOLS)
    fig1.line(t00, lme_cu_3m, line_width=2, line_color='black', legend_label='期货收盘价:LME3个月铜')
    fig1.y_range = Range1d(np.min(lme_cu_3m)*0.9, np.max(lme_cu_3m)*1.1)
    y_column2_name = 'y2'
    fig1.extra_y_ranges = {
        y_column2_name: Range1d(
            start=np.min(usdcny)*0.9,
            end=np.max(usdcny)*1.1,
        ),
    }
    fig1.line(t01, usdcny, line_width=2, color='blue', y_range_name=y_column2_name, legend_label='USDCNY')
    fig1.add_layout(LinearAxis(y_range_name="y2"), 'right')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"
    show(fig1)

# BCOM
def test16():
    start_time = '2005-1-1'
    end_time = '2029-12-31'

    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    lme_cu_3m = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    t, lme_cu_3m = get_period_data(t, lme_cu_3m, start_time, end_time, remove_nan=True)

    cftc_plot_financial(t, lme_cu_3m, 'LME3个月铜', code='221602', inst_name='CBT:BCOM')


# CU/CNY 和 CU/USD
def test17():
    path = os.path.join(data_dir, '铜'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    cu_price_usd = np.array(df['期货收盘价:LME3个月铜'], dtype=float)
    cu_price_cny = np.array(df['期货收盘价(主力):阴极铜'], dtype=float)
    t1, cu_price_cny = get_period_data(t, cu_price_cny, start_time, end_time, remove_nan=True)
    t2, cu_price_usd = get_period_data(t, cu_price_usd, start_time, end_time, remove_nan=True)

    path = os.path.join(data_dir, '汇率'+'.csv') 
    df1 = pd.read_csv(path)
    df1 = df1[['time', '即期汇率:美元兑人民币']].dropna()
    dxy_t = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    usdcny = np.array(df1['即期汇率:美元兑人民币'], dtype=float)

    cu_price_cny = cu_price_cny/1.13 # 关税
    t1, cu_price_cny_to_usd = data_div(t1, cu_price_cny, dxy_t, usdcny)

    t3, diff = data_sub(t1, cu_price_cny_to_usd, t2, cu_price_usd)

    fig1 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, cu_price_cny_to_usd, line_width=2, line_color='blue', legend_label='cu cny to usd')
    fig1.line(t2, cu_price_usd, line_width=2, line_color='orange', legend_label='cu usd')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    fig2 = figure(frame_width=1400, frame_height=300, tools=TOOLS, x_axis_type = "datetime", x_range=fig1.x_range, y_axis_location="right")
    fig2.line(t3, diff, line_width=2, line_color='blue', legend_label='CNY 溢价')
    fig2.xaxis[0].ticker.desired_num_ticks = 20
    fig2.legend.click_policy="hide"

    show(column(fig1,fig2))


if __name__=="__main__":
    # 仓位
    test1()
    test11()

    # 库存
    test2()

    # 铜和PMI
    test3()

    # # # # # 铜和MSCI指数
    # test33()

    # # # # # 精炼铜进口
    # # # # test4()

    # 现货升贴水
    test5()
    time.sleep(0.25)

    # TC/RC
    test6()

    # # # # 铜需求
    # # # test66()

    # 铜金比 和 利率
    test12()

    # LME 0-3
    # test13()

    # 澳元
    test14()

    # 人民币
    test15()

    # # 彭博商品指数
    # test16()

    # CU/CNY 和 CU/USD
    test17()

    # 
    plot_option_position_basis_data('shfe', 'cu')

# 升贴水指的是期货与现货的差价,不单指现货或期货。 
# 指LME铜现货价对3个月期货价的升贴水，计算方法是升贴水(0-3)=现货收盘价格(场内盘)-三个月期货收盘价(场内盘),
# 即LME铜升贴水(0-3)=现货收盘价(场内盘):LME铜-期货收盘价(场内盘):LME铜
# 升贴水(0-3)”指现货价对3个月期货价的升贴水。
# 升贴水(3-15)”指3个月期货价对15个月期货价的升贴水。


# "洋山铜溢价"是指以待报关进口至上海(包括洋山和外高桥)的基准品牌精炼铜交易价格对伦敦期货交易所(LME)三月铜的当日溢价。 
# 反应的是由于洋山（间接反应上海、中国）现货铜的供需导致购买现货铜需要付出的超过铜价本身（LME三月期货铜价）的超额部分单价。


# "铜升贴水 长江有色市场"是现货铜价格与沪期铜当月期货价的差值；指标算法中沪期铜当月期货价，取的是最近月份合约的期货价，期货价格为收盘价


# "1#铜升贴水"表示现货价格与沪期铜当月即时卖价所形成的价差区间，取的是11:30的值


# 库存小计为符合交割品质的货物数量，库存期货为已制成仓单的货物数量；可用库容为可制成仓单的数量。

# 陆上风电每GW耗铜量0.54万吨，
# 海上风每GW耗铜量1.53万吨铜；
# 其他投资机构数据显示，
# 陆上风电每GW耗铜量0.4万吨，
# 海上风电每GW耗铜量1万吨。
# 结合两家统计取均值，
# 预计海上风电每GW装机需要消耗1.26万吨铜，
# 陆上风电每GW耗铜量0.47万吨，
# 风电耗铜量中值约为0.9万吨/GW。
# Navigant Research的数据为
# 光电每GW耗铜量0.55万吨，
# Joint Research Centre数据为
# 每GW耗铜量0.46万吨，也有资料认为光电每GW耗铜量0.4万吨，光电的耗铜量中值约为0.45万吨/GW。



# 举例来说：进口铜精矿含铜30%，回收率 96.5%，TC为45美元/吨，RC 4.5 美分/磅，计算如下：(1 吨 =2204.62 磅，1 美元 =100 美分 )
# 粗炼费＝1÷Cu%÷回收率 %×TC =1÷30%÷96.5%×45=155.44 美元/吨铜
# 精炼费＝RC×2204.62 ÷100＝ 4.5 ×2204.62 ÷100＝99.21 美元/吨铜
# 由此，综合加工费＝粗炼费＋精炼费＝155.44＋99.21＝254.65 美元/吨铜



# 一台空调含铜：8.3kg(估计)
# 一台冰箱含铜：

# 一台纯电动车含铜：83kg