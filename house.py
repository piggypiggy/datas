import os
import time
import numpy as np
import pandas as pd
import datetime
from utils import *
from math import pi
from chinamoney import *

start_time = '2013-01-01'
end_time = '2099-12-31'

# 成交面积
def test1():
    path = os.path.join(data_dir, '房地产'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))

    z1 = np.array(df['房屋施工面积：当月值'], dtype=float)/10000
    z2 = np.array(df['房屋新开工面积：当月值'], dtype=float)
    z22 = np.array(df['城镇房屋竣工面积：当月值'], dtype=float)
    z23 = np.array(df['全国:成交土地土地出让金:总计:累计值'], dtype=float)   
    z24 = np.array(df['房地产开发投资完成额:累计值'], dtype=float)   

    t1, z1 = get_period_data(t, z1, start_time, end_time, remove_nan=True)
    t2, z2 = get_period_data(t, z2, start_time, end_time, remove_nan=True)
    t22, z22 = get_period_data(t, z22, start_time, end_time, remove_nan=True)
    t23, z23 = get_period_data(t, z23, start_time, end_time, remove_nan=True)
    t24, z24 = get_period_data(t, z24, start_time, end_time, remove_nan=True)

    plot_seasonality(t1, z1, start_year=2013, title='房屋施工面积：当月值 (万平方米)')
    plot_seasonality(t2, z2, start_year=2013, title='房屋新开工面积：当月值 (万平方米)')
    plot_seasonality(t22, z22, start_year=2013, title='城镇房屋竣工面积：当月值')
    plot_seasonality(t23, z23, start_year=2013, title='全国:成交土地土地出让金:总计:累计值')
    plot_seasonality(t24, z24, start_year=2013, title='房地产开发投资完成额:累计值')

    z3 = np.array(df['30大中城市:商品房成交面积'], dtype=float)
    z4 = np.array(df['30大中城市:商品房成交面积:一线城市'], dtype=float)
    z5 = np.array(df['30大中城市:商品房成交面积:二线城市'], dtype=float)
    z6 = np.array(df['30大中城市:商品房成交面积:三线城市'], dtype=float)

    t3, z3 = get_period_data(t, z3, start_time, end_time, remove_nan=True)
    t4, z4 = get_period_data(t, z4, start_time, end_time, remove_nan=True)
    t5, z5 = get_period_data(t, z5, start_time, end_time, remove_nan=True)
    t6, z6 = get_period_data(t, z6, start_time, end_time, remove_nan=True)

    t3, z3 = moving_average(t3, z3, 7)
    t4, z4 = moving_average(t4, z4, 7)
    t5, z5 = moving_average(t5, z5, 7)
    t6, z6 = moving_average(t6, z6, 7)

    plot_seasonality(t3, z3, start_year=2013, title='30大中城市:商品房成交面积 (万平方米)')
    plot_seasonality(t4, z4, start_year=2013, title='30大中城市:商品房成交面积:一线城市 (万平方米)')
    plot_seasonality(t5, z5, start_year=2013, title='30大中城市:商品房成交面积:二线城市 (万平方米)')
    plot_seasonality(t6, z6, start_year=2013, title='30大中城市:商品房成交面积:三线城市 (万平方米)')
    time.sleep(0.25)

    z7 = np.array(df['新建商品住宅价格指数：同比：上年同月=100'], dtype=float)
    z8 = np.array(df['新建商品住宅价格指数：一线城市：同比：上年同月=100'], dtype=float)
    z9 = np.array(df['新建商品住宅价格指数：二线城市：同比：上年同月=100'], dtype=float)
    z10 = np.array(df['新建商品住宅价格指数：三线城市：同比：上年同月=100'], dtype=float)
    datas = [[t, z7, '新建商品住宅价格指数：同比'],
             [t, z8, '新建商品住宅价格指数：一线城市：同比'],
             [t, z9, '新建商品住宅价格指数：二线城市：同比'],
             [t, z10, '新建商品住宅价格指数：三线城市：同比'],]
    plot_one_figure(datas)


# 中资地产美元债ETF
def test2():
    path = os.path.join(data_dir, '中国房地产美元债 ETF'+'.csv') 
    df = pd.read_csv(path)

    df["time"] = pd.to_datetime(df["time"])

    inc = df.close > df.open
    dec = df.open > df.close
    w = 12*60*60*1000 # half day in ms

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

    p = figure(x_axis_type="datetime", tools=TOOLS, frame_width=1400, frame_height=500, title = "中国房地产美元债 ETF")
    p.xaxis.major_label_orientation = pi/4
    p.grid.grid_line_alpha=0.3

    p.segment(df.time, df.high, df.time, df.low, color="black")
    p.vbar(df.time[inc], w, df.open[inc], df.close[inc], fill_color="green", line_color="black")
    p.vbar(df.time[dec], w, df.open[dec], df.close[dec], fill_color="red", line_color="black")
    p.xaxis[0].ticker.desired_num_ticks = 30
    show(p)


# RMBS条件早偿率指数
def test3():
    path = os.path.join(data_dir, 'RMBS条件早偿率指数'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['rate'], dtype=float)

    datas = [[t, data, 'RMBS条件早偿率指数']]
    plot_one_figure(datas)


# 统计局数据
def test4():
    path = os.path.join(nbs_dir, '房地产(统计局)'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))
    data1 = np.array(df['房地产住宅投资_累计值'], dtype=float)
    data2 = np.array(df['房地产住宅投资_累计增长'], dtype=float)
    data3 = np.array(df['房地产开发新增固定资产投资_累计值'], dtype=float)
    data4 = np.array(df['房地产开发新增固定资产投资_累计增长'], dtype=float)
    data5 = np.array(df['房地产开发计划总投资_累计值'], dtype=float)
    data6 = np.array(df['房地产开发计划总投资_累计增长'], dtype=float)

    time.sleep(0.5)
    datas = [[[[t, data1, '房地产住宅投资_累计值', '']],
              [[t, data2, '房地产住宅投资_累计增长', '']],'']]
    plot_many_figure(datas)
    plot_seasonality(t, data1, start_year=2012, end_year=2099, title='房地产住宅投资_累计值')

    time.sleep(0.5)
    datas = [[[[t, data3, '房地产开发新增固定资产投资_累计值', '']],
              [[t, data4, '房地产开发新增固定资产投资_累计增长', '']],'']]
    plot_many_figure(datas)
    plot_seasonality(t, data3, start_year=2012, end_year=2099, title='房地产开发新增固定资产投资_累计值')

    time.sleep(0.5)
    datas = [[[[t, data5, '房地产开发计划总投资_累计值', '']],
              [[t, data6, '房地产开发计划总投资_累计增长', '']],'']]
    plot_many_figure(datas)
    # plot_seasonality(t, data5, start_year=2012, end_year=2099, title='房地产开发计划总投资_累计值')


# 70个大中城市住宅销售价格指数
def test5():
    path = os.path.join(nbs_dir, '70个大中城市住宅销售价格指数'+'.csv') 
    df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(df['time']['time'], format='%Y-%m'))

    # 上涨、持平、下跌城市个数
    mom_new_hi_count, mom_new_eq_count, mom_new_lo_count = get_cs_price_change_count(df, '新建商品住宅销售价格指数(上月=100)')
    yoy_new_hi_count, yoy_new_eq_count, yoy_new_lo_count = get_cs_price_change_count(df, '新建商品住宅销售价格指数(上年同月=100)')
    mom_seh_hi_count, mom_seh_eq_count, mom_seh_lo_count = get_cs_price_change_count(df, '二手住宅销售价格指数(上月=100)')
    yoy_seh_hi_count, yoy_seh_eq_count, yoy_seh_lo_count = get_cs_price_change_count(df, '二手住宅销售价格指数(上年同月=100)')

    mom_90new_hi_count, mom_90new_eq_count, mom_90new_lo_count = get_cs_price_change_count(df, '90平米及以下新建商品住宅销售价格指数(上月=100)')
    yoy_90new_hi_count, yoy_90new_eq_count, yoy_90new_lo_count = get_cs_price_change_count(df, '90平米及以下新建商品住宅销售价格指数(上年同月=100)')
    mom_90seh_hi_count, mom_90seh_eq_count, mom_90seh_lo_count = get_cs_price_change_count(df, '90平米及以下二手住宅销售价格指数(上月=100)')
    yoy_90seh_hi_count, yoy_90seh_eq_count, yoy_90seh_lo_count = get_cs_price_change_count(df, '90平米及以下二手住宅销售价格指数(上年同月=100)')

    mom_90_144new_hi_count, mom_90_144new_eq_count, mom_90_144new_lo_count = get_cs_price_change_count(df, '90-144平米新建商品住宅销售价格指数(上月=100)')
    yoy_90_144new_hi_count, yoy_90_144new_eq_count, yoy_90_144new_lo_count = get_cs_price_change_count(df, '90-144平米新建商品住宅销售价格指数(上年同月=100)')
    mom_90_144seh_hi_count, mom_90_144seh_eq_count, mom_90_144seh_lo_count = get_cs_price_change_count(df, '90-144平米二手住宅销售价格指数(上月=100)')
    yoy_90_144seh_hi_count, yoy_90_144seh_eq_count, yoy_90_144seh_lo_count = get_cs_price_change_count(df, '90-144平米二手住宅销售价格指数(上年同月=100)')

    mom_144new_hi_count, mom_144new_eq_count, mom_144new_lo_count = get_cs_price_change_count(df, '144平米以上新建商品住宅销售价格指数(上月=100)')
    yoy_144new_hi_count, yoy_144new_eq_count, yoy_144new_lo_count = get_cs_price_change_count(df, '144平米以上新建商品住宅销售价格指数(上年同月=100)')
    mom_144seh_hi_count, mom_144seh_eq_count, mom_144seh_lo_count = get_cs_price_change_count(df, '144平米以上二手住宅销售价格指数(上月=100)')
    yoy_144seh_hi_count, yoy_144seh_eq_count, yoy_144seh_lo_count = get_cs_price_change_count(df, '144平米以上二手住宅销售价格指数(上年同月=100)')

    datas = [[[[t, mom_new_hi_count, '新建商品住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_new_eq_count, '新建商品住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_new_lo_count, '新建商品住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_new_hi_count, '新建商品住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_new_eq_count, '新建商品住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_new_lo_count, '新建商品住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_seh_hi_count, '二手住宅住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_seh_eq_count, '二手住宅住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_seh_lo_count, '二手住宅住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_seh_hi_count, '二手住宅住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_seh_eq_count, '二手住宅住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_seh_lo_count, '二手住宅住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_90new_hi_count, '90平米及以下新建商品住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_90new_eq_count, '90平米及以下新建商品住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_90new_lo_count, '90平米及以下新建商品住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_90new_hi_count, '90平米及以下新建商品住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_90new_eq_count, '90平米及以下新建商品住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_90new_lo_count, '90平米及以下新建商品住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_90seh_hi_count, '90平米及以下二手住宅住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_90seh_eq_count, '90平米及以下二手住宅住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_90seh_lo_count, '90平米及以下二手住宅住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_90seh_hi_count, '90平米及以下二手住宅住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_90seh_eq_count, '90平米及以下二手住宅住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_90seh_lo_count, '90平米及以下二手住宅住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_90_144new_hi_count, '90-144平米新建商品住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_90_144new_eq_count, '90-144平米新建商品住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_90_144new_lo_count, '90-144平米新建商品住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_90_144new_hi_count, '90-144平米新建商品住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_90_144new_eq_count, '90-144平米新建商品住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_90_144new_lo_count, '90-144平米新建商品住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_90_144seh_hi_count, '90-144平米二手住宅住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_90_144seh_eq_count, '90-144平米二手住宅住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_90_144seh_lo_count, '90-144平米二手住宅住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_90_144seh_hi_count, '90-144平米二手住宅住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_90_144seh_eq_count, '90-144平米二手住宅住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_90_144seh_lo_count, '90-144平米二手住宅住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_144new_hi_count, '144平米以上新建商品住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_144new_eq_count, '144平米以上新建商品住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_144new_lo_count, '144平米以上新建商品住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_144new_hi_count, '144平米以上新建商品住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_144new_eq_count, '144平米以上新建商品住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_144new_lo_count, '144平米以上新建商品住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)

    datas = [[[[t, mom_144seh_hi_count, '144平米以上二手住宅住宅销售价格 环比上涨个数', 'color=red'],
               [t, mom_144seh_eq_count, '144平米以上二手住宅住宅销售价格 环比不变个数', 'color=gray'],
               [t, mom_144seh_lo_count, '144平米以上二手住宅住宅销售价格 环比下跌个数', 'color=darkgreen']],
              [],'70个大中城市'],
              [[[t, yoy_144seh_hi_count, '144平米以上二手住宅住宅销售价格 同比上涨个数', 'color=red'],
               [t, yoy_144seh_eq_count, '144平米以上二手住宅住宅销售价格 同比不变个数', 'color=gray'],
               [t, yoy_144seh_lo_count, '144平米以上二手住宅住宅销售价格 同比下跌个数', 'color=darkgreen']],
              [],'']
              ]
    plot_many_figure(datas)


if __name__=="__main__":
    # # 30个大中城市成交面积
    test1()

    # # 中国地产美元债ETF
    # test2()

    # # RMBS条件早偿率指数
    # plot_rmbs()

    # # 房地产(统计局)
    # test4()

    # 70个大中城市住宅销售价格指数
    # test5()

    pass
