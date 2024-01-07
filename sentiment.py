import os
import time
import numpy as np
import pandas as pd
import datetime
from utils import *

start_time = '2005-1-1'
end_time = '2028-12-31'


def test1():
    path = os.path.join(data_dir, '情绪'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data1 = np.array(df['消费者信心指数'], dtype=float)
    data2 = np.array(df['消费者满意指数'], dtype=float)
    data3 = np.array(df['消费者预期指数'], dtype=float)
    t1, data1 = get_period_data(t, data1, start_time, end_time, remove_nan=True)
    t2, data2 = get_period_data(t, data2, start_time, end_time, remove_nan=True)
    t3, data3 = get_period_data(t, data3, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1400, frame_height=500, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, data1, line_width=2, color='orange', legend_label='消费者信心指数')
    fig1.line(t2, data2, line_width=2, color='blue', legend_label='消费者满意指数')
    fig1.line(t3, data3, line_width=2, color='darkgreen', legend_label='消费者预期指数')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    show(fig1)
    time.sleep(0.25)

    data1 = np.array(df['中小企业发展指数:总指数'], dtype=float)
    data2 = np.array(df['中小企业发展指数:宏观经济感受指数'], dtype=float)
    t1, data1 = get_period_data(t, data1, start_time, end_time, remove_nan=True)
    t2, data2 = get_period_data(t, data2, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1400, frame_height=500, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, data1, line_width=2, color='orange', legend_label='中小企业发展指数:总指数')
    fig1.line(t2, data2, line_width=2, color='blue', legend_label='中小企业发展指数:宏观经济感受指数')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    show(fig1)
    time.sleep(0.25)

    data1 = np.array(df['长江商学院中国企业经营状况指数(长江商学院BCI)'], dtype=float)
    data2 = np.array(df['长江商学院BCI之企业利润前瞻指数'], dtype=float)
    data3 = np.array(df['长江商学院企业招工前瞻指数'], dtype=float)
    t1, data1 = get_period_data(t, data1, start_time, end_time, remove_nan=True)
    t2, data2 = get_period_data(t, data2, start_time, end_time, remove_nan=True)
    t3, data3 = get_period_data(t, data3, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1400, frame_height=500, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, data1, line_width=2, color='orange', legend_label='长江商学院中国企业经营状况指数(长江商学院BCI)')
    fig1.line(t2, data2, line_width=2, color='blue', legend_label='长江商学院BCI之企业利润前瞻指数')
    fig1.line(t3, data3, line_width=2, color='darkgreen', legend_label='长江商学院企业招工前瞻指数')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    show(fig1)
    time.sleep(0.25)

    data1 = np.array(df['美国:密歇根大学消费者信心指数:初值'], dtype=float)
    data2 = np.array(df['美国:密歇根大学现况指数:初值'], dtype=float)
    data3 = np.array(df['美国:密歇根大学预期指数:初值'], dtype=float)
    t1, data1 = get_period_data(t, data1, start_time, end_time, remove_nan=True)
    t2, data2 = get_period_data(t, data2, start_time, end_time, remove_nan=True)
    t3, data3 = get_period_data(t, data3, start_time, end_time, remove_nan=True)

    fig1 = figure(frame_width=1400, frame_height=500, tools=TOOLS, x_axis_type = "datetime", y_axis_location="right")
    fig1.line(t1, data1, line_width=2, color='orange', legend_label='美国:密歇根大学消费者信心指数:初值')
    fig1.line(t2, data2, line_width=2, color='blue', legend_label='美国:密歇根大学现况指数:初值')
    fig1.line(t3, data3, line_width=2, color='darkgreen', legend_label='美国:密歇根大学预期指数:初值')
    fig1.xaxis[0].ticker.desired_num_ticks = 20
    fig1.legend.click_policy="hide"

    show(fig1)


def test2():
    path = os.path.join(data_dir, '情绪'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data1 = np.array(df['花旗新兴市场经济意外指数'], dtype=float)
    data2 = np.array(df['花旗十国集团经济意外指数'], dtype=float)
    data3 = np.array(df['花旗日本经济意外指数'], dtype=float)
    data4 = np.array(df['花旗中国经济意外指数'], dtype=float)
    data5 = np.array(df['花旗欧洲经济意外指数'], dtype=float)
    data6 = np.array(df['花旗美国经济意外指数'], dtype=float)

    datas = [[[[t,data1,'花旗新兴市场经济意外指数',''], 
               [t,data2,'花旗十国集团经济意外指数',''], 
               [t,data3,'花旗日本经济意外指数','']],[],'']]
    plot_many_figure(datas)

    t2, diff2 = data_sub(t, data6, t, data5)
    t3, diff3 = data_sub(t, data6, t, data4)
    datas = [[[[t,data4,'花旗中国经济意外指数','color=red'], [t,data5,'花旗欧洲经济意外指数',''], [t,data6,'花旗美国经济意外指数','']],[],''],
             [[[t2,diff2,'US-EU','']],[],''],
             [[[t3,diff3,'US-CN','']],[],'']]
    plot_many_figure(datas)

    # path = os.path.join(data_dir, '股指'+'.csv') 
    # df = pd.read_csv(path)

    # t0 = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    # sz50 = np.array(df['上证50_x'], dtype=float)
    # cs300 = np.array(df['沪深300_x'], dtype=float)
    # zz500 = np.array(df['中证500_x'], dtype=float)

    # t01,sz50_yoy = yoy(t0, sz50)
    # t02,cs300_yoy = yoy(t0, cs300)
    # t03,zz500_yoy = yoy(t0, zz500)

    # datas = [[[[t,data1,'花旗新兴市场经济意外指数',''], [t,data2,'花旗十国集团经济意外指数',''], [t3,diff3,'US-CN','']],[],''],
    #          [[[t02,cs300_yoy,'cs300_yoy','']],[],''],
    #          ]
    # plot_many_figure(datas)


# 国内失业率
def test3():
    path = os.path.join(nbs_dir, '城镇调查失业率'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    data1 = np.array(df['全国本地户籍人口城镇调查失业率'], dtype=float)
    data2 = np.array(df['全国外来户籍人口城镇调查失业率'], dtype=float)
    data3 = np.array(df['全国城镇调查失业率'], dtype=float)
    data4 = np.array(df['全国16-24岁人口城镇调查失业率'], dtype=float)
    data5 = np.array(df['全国25-59岁人口城镇调查失业率'], dtype=float)
    data6 = np.array(df['企业就业人员周平均工作时间'], dtype=float)
    data7 = np.array(df['31个大城市城镇调查失业率'], dtype=float)

    datas = [[[[t,data1,'全国本地户籍人口城镇调查失业率',''],
               [t,data2,'全国外来户籍人口城镇调查失业率',''],
               [t,data3,'全国城镇调查失业率',''],
               [t,data4,'全国16-24岁人口城镇调查失业率',''],
               [t,data5,'全国25-59岁人口城镇调查失业率',''],
               [t,data7,'31个大城市城镇调查失业率',''],],
              [],''],]
    plot_many_figure(datas, max_height=700)

    time.sleep(0.5)
    plot_seasonality(t, data4, start_year=2012, title='全国16-24岁人口城镇调查失业率')



def plot_consumption_vs_income():
    path = os.path.join(nbs_dir, '人民生活'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m'))

    consume = np.array(df['城镇居民人均消费支出_累计值'], dtype=float)
    income = np.array(df['城镇居民人均可支配收入_累计值'], dtype=float)
    datas = [[[[t,consume,'城镇居民人均消费支出_累计值',''],
               [t,income,'城镇居民人均可支配收入_累计值',''],],
              [],''],
              
             [[[t,consume/income,'城镇居民人均消费支出 / 城镇居民人均可支配收入',''],],
              [],''],]
    plot_many_figure(datas, max_height=700)


if __name__=="__main__":
    # 消费者信心指数
    test1()

    # 花旗经济意外指数
    test2()

    # 失业率
    test3()

    plot_consumption_vs_income()
    pass
