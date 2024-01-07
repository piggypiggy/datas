import os
import numpy as np
import pandas as pd
import datetime
from utils import *
from cftc import *
from lme import *
from compare import *

start_time = '2010-1-1'
end_time = '2099-12-31'


def plot_c():
    # 玉米
    path = os.path.join(future_price_dir, 'dce', 'c'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'])
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'DCE:玉米', code='002602', inst_name='CBOT:玉米')

    path = os.path.join(cfd_dir, 'CORN_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'CORN CFD', code='002602', inst_name='CBOT:玉米')

    # U.S. cents per bushel
    adjust = 0.01 / 0.0254012
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='玉米')


def plot_a():
    # 豆一
    path = os.path.join(future_price_dir, 'dce', 'a'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'])
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'DCE:豆一', code='005602', inst_name='CBOT:大豆')

    path = os.path.join(cfd_dir, 'SOYBEAN_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'SOYBEAN CFD', code='005602', inst_name='CBOT:大豆')

    # U.S. cents per bushel
    adjust = 0.01 / 0.0272
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='豆一')


def plot_b():
    # 豆二
    path = os.path.join(future_price_dir, 'dce', 'b'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'])
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'DCE:豆二', code='005602', inst_name='CBOT:大豆')

    path = os.path.join(cfd_dir, 'SOYBEAN_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'SOYBEAN CFD', code='005602', inst_name='CBOT:大豆')

    # U.S. cents per bushel
    adjust = 0.01 / 0.0272
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='豆二')


def plot_m():
    # 豆粕
    path = os.path.join(future_price_dir, 'dce', 'm'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'])
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'DCE:豆粕', code='026603', inst_name='CBOT:豆粕')

    path = os.path.join(cfd_dir, 'SOYBEAN_MEAL_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'SOYBEAN MEAL CFD', code='026603', inst_name='CBOT:豆粕')

    # U.S. dollars per short ton
    adjust = 0.9071847
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='豆粕')


def plot_y():
    # 豆油
    path = os.path.join(future_price_dir, 'dce', 'y'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'])
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'DCE:豆油', code='007601', inst_name='CBOT:豆油')

    path = os.path.join(cfd_dir, 'SOYBEAN_OIL_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'SOYBEAN OIL CFD', code='007601', inst_name='CBOT:豆油')

    # U.S. cents per pound
    adjust = 0.01  / 0.00045359237
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='豆油')


def plot_cf():
    # 棉花
    path = os.path.join(future_price_dir, 'czce', 'CF'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'])
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'CZCE:棉花', code='033661', inst_name='ICE:棉花')

    path = os.path.join(cfd_dir, 'COTTON_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'COTTON CFD', code='033661', inst_name='ICE:棉花')

    # U.S. cents per pound
    adjust = 0.01 / 0.00045359237
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='棉花')


# def plot_p():
#     # 棕榈油
#     path = os.path.join(future_price_dir, 'dce', 'p'+'.csv')
#     fut_df = pd.read_csv(path, header=[0,1])
#     t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
#     price = np.array(fut_df['index']['close'])
#     t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
#     cftc_plot_disaggregated(t0, price0, 'DCE:棕榈油', code='037021', inst_name='CME:棕榈油')


def plot_sr():
    # 白糖
    path2 = os.path.join(future_price_dir, 'czce', 'SR'+'.csv')
    fut_df = pd.read_csv(path2, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    price = np.array(fut_df['index']['close'], dtype=float)
    t0, price0 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, price0, 'CZCE:白砂糖', code='080732', inst_name='ICE:糖')

    path = os.path.join(cfd_dir, 'SUGAR_CFD'+'.csv')
    fut_df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time'], format='%Y-%m-%d'))
    price = np.array(fut_df['close'])
    t1, price1 = get_period_data(t, price, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t1, price1, 'SUGAR CFD', code='080732', inst_name='ICE:糖')

    # U.S. cents per pound
    adjust = 0.01  / 0.00045359237
    compare_price_in_different_currency(t1, price1, 'USD', t0, price0, 'CNY', adjust, variety='糖')


def plot_agricuture():
    plot_a()
    plot_b()
    plot_m()
    plot_y()
    plot_c()
    plot_cf()
    plot_sr()


if __name__=="__main__":
    plot_agricuture()

