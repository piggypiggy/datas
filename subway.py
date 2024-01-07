import os
import time
import numpy as np
import pandas as pd
import datetime
from utils import *


start_time = '2000-1-1'
end_time = '2029-12-31'

def test1():
    path = os.path.join(data_dir, '地铁'+'.csv') 
    df = pd.read_csv(path)

    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    beijing = np.array(df['地铁客流量:北京'], dtype=float)
    shanghai = np.array(df['地铁客流量:上海'], dtype=float)
    guangzhou = np.array(df['地铁客流量:广州'], dtype=float)
    shenzhen = np.array(df['地铁客流量:深圳'], dtype=float)

    chengdu = np.array(df['地铁客流量:成都'], dtype=float)
    xian = np.array(df['地铁客流量:西安'], dtype=float)
    zhengzhou = np.array(df['地铁客流量:郑州'], dtype=float)
    hefei = np.array(df['地铁客流量:合肥'], dtype=float)
    hangzhou = np.array(df['地铁客流量:杭州'], dtype=float)
    kunmin = np.array(df['地铁客流量:昆明'], dtype=float)


    t1, beijing = get_period_data(t, beijing, start_time, end_time, remove_nan=True)
    t2, shanghai = get_period_data(t, shanghai, start_time, end_time, remove_nan=True)
    t3, guangzhou = get_period_data(t, guangzhou, start_time, end_time, remove_nan=True)
    t4, shenzhen = get_period_data(t, shenzhen, start_time, end_time, remove_nan=True)
    t5, chengdu = get_period_data(t, chengdu, start_time, end_time, remove_nan=True)
    t6, xian = get_period_data(t, xian, start_time, end_time, remove_nan=True)
    t7, zhengzhou = get_period_data(t, zhengzhou, start_time, end_time, remove_nan=True)
    t8, hefei = get_period_data(t, hefei, start_time, end_time, remove_nan=True)
    t9, hangzhou = get_period_data(t, hangzhou, start_time, end_time, remove_nan=True)
    t10, kunmin = get_period_data(t, kunmin, start_time, end_time, remove_nan=True)

    t1, beijing = moving_average(t1, beijing, 7)
    t2, shanghai = moving_average(t2, shanghai, 7)
    t3, guangzhou = moving_average(t3, guangzhou, 7)
    t4, shenzhen = moving_average(t4, shenzhen, 7)
    t5, chengdu = moving_average(t5, chengdu, 7)
    t6, xian = moving_average(t6, xian, 7)
    t7, zhengzhou = moving_average(t7, zhengzhou, 7)
    t8, hefei = moving_average(t8, hefei, 7)
    t9, hangzhou = moving_average(t7, hangzhou, 7)
    t10, kunmin = moving_average(t8, kunmin, 7)

    plot_seasonality(t1, beijing, title='地铁客流量:北京 7dma')
    plot_seasonality(t2, shanghai, title='地铁客流量:上海 7dma')
    plot_seasonality(t3, guangzhou, title='地铁客流量:广州 7dma')
    plot_seasonality(t4, shenzhen, title='地铁客流量:深圳 7dma')
    plot_seasonality(t5, chengdu, title='地铁客流量:成都 7dma')
    plot_seasonality(t6, xian, title='地铁客流量:西安 7dma')
    plot_seasonality(t7, zhengzhou, title='地铁客流量:郑州 7dma')
    plot_seasonality(t8, hefei, title='地铁客流量:合肥 7dma')
    # plot_seasonality(t9, hangzhou, title='地铁客流量:杭州 7dma')
    plot_seasonality(t10, kunmin, title='地铁客流量:昆明 7dma')


if __name__=="__main__":
    test1()
