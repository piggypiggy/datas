import os
import numpy as np
import pandas as pd
import datetime
from utils import *
from cftc import *
from lme import *
from option import *
from nasdaq import *

start_time = '2012-1-1'
end_time = '2099-12-31'


def metal_au():
    path = os.path.join(cfd_dir, 'GOLD_CFD'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['close'], dtype=float)
    cftc_plot_disaggregated(t, data, 'GOLD CFD', code='088691', inst_name='COMEX:黄金')

    plot_nasdaq_option_datas('GLD')

    path = os.path.join(data_dir, 'vix'+'.csv') 
    df2 = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    vix = np.array(df2['oil_etf_vix'], dtype=float)

    datas = [[[[t,data,'GOLD CFD','color=black'],],[[t2,vix,'GOLD ETF VIX',''],],''],]
    plot_many_figure(datas)


def metal_ag():
    path = os.path.join(cfd_dir, 'SILVER_CFD'+'.csv') 
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['close'], dtype=float)
    cftc_plot_disaggregated(t, data, 'SILVER CFD', code='084691', inst_name='COMEX:白银')

    plot_nasdaq_option_datas('SLV')


def metal_cu():
    # 铜
    path = os.path.join(future_price_dir, 'shfe', 'cu'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    copper = np.array(fut_df['index']['close'])
    t0, copper = get_period_data(t, copper, start_time, end_time, remove_nan=True)
    cftc_plot_disaggregated(t0, copper, '沪铜指数', code='085692', inst_name='CME:铜')
    lme_plot_position(t0, copper, '沪铜指数', code='Copper', inst_name='LME:铜')
    plot_metal_stock('cu', '铜')
    # plot_option_position_basis_data('shfe', 'cu')


def metal_al():
    # 铅
    path = os.path.join(future_price_dir, 'shfe', 'al'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    al = np.array(fut_df['index']['close'])
    t0, al = get_period_data(t, al, start_time, end_time, remove_nan=True)
    lme_plot_position(t0, al, '沪铝指数', code='Aluminium', inst_name='LME:铝')
    plot_metal_stock('al', '铝')
    # plot_option_position_basis_data('shfe', 'al')


def metal_pb():    
    # 铅
    path = os.path.join(future_price_dir, 'shfe', 'pb'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    lead = np.array(fut_df['index']['close'])
    t0, lead = get_period_data(t, lead, start_time, end_time, remove_nan=True)
    lme_plot_position(t0, lead, '沪铅指数', code='Lead', inst_name='LME:铅')
    plot_metal_stock('pb', '铅')
    # plot_option_position_basis_data('shfe', 'pb')


def metal_zn():  
    # 锌
    path = os.path.join(future_price_dir, 'shfe', 'zn'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    zinc = np.array(fut_df['index']['close'])
    t0, zinc = get_period_data(t, zinc, start_time, end_time, remove_nan=True)
    lme_plot_position(t0, zinc, '沪锌指数', code='Zinc', inst_name='LME:锌')
    plot_metal_stock('zn', '锌')
    # plot_option_position_basis_data('shfe', 'zn')


def metal_ni():  
    # 镍
    path = os.path.join(future_price_dir, 'shfe', 'ni'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    nickel = np.array(fut_df['index']['close'])
    t0, nickel = get_period_data(t, nickel, start_time, end_time, remove_nan=True)
    lme_plot_position(t0, nickel, '沪镍指数', code='Nickel', inst_name='LME:镍')
    plot_metal_stock('ni', '镍')
    # plot_option_position_basis_data('shfe', 'ni')


def metal_sn():  
    # # 锡
    path = os.path.join(future_price_dir, 'shfe', 'sn'+'.csv')
    fut_df = pd.read_csv(path, header=[0,1])
    t = pd.DatetimeIndex(pd.to_datetime(fut_df['time']['Unnamed: 0_level_1'], format='%Y-%m-%d'))
    tin = np.array(fut_df['index']['close'])
    t0, tin = get_period_data(t, tin, start_time, end_time, remove_nan=True)
    lme_plot_position(t0, tin, '沪锡指数', code='Tin', inst_name='LME:锡')
    plot_metal_stock('sn', '锡')
    # plot_option_position_basis_data('shfe', 'sn')

    # SOXX, SN
    t1, soxx = read_us_etf_data('SOXX', fq=False)
    datas = [[t0,tin,'SN',''], 
             [t1,soxx,'SOXX','']]
    compare_two_data(datas, start_time='2020-01-01')


# 金属
def test1():
    # metal_au()
    # metal_ag()

    metal_cu()
    metal_al()
    metal_pb()
    metal_zn()
    metal_ni()
    metal_sn()

    pass

    
if __name__=="__main__":
    test1()
