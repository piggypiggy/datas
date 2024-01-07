import os
import numpy as np
import pandas as pd
import datetime
from utils import *


def update_vix():
    code = [
        ['VIXCLS', 'vix'],
        ['GVZCLS', 'gold_etf_vix'],
        ['OVXCLS', 'oil_etf_vix'],
        ['VXEEMCLS', 'em_etf_vix'],
        ['VXNCLS', 'nasdaq100_vix'],
        ['VXDCLS', 'djia_vix'],
        ['VXEWZCLS', 'brazil_etf_vix'],
    ]

    name_code = {'vix': code}
    update_fred_data(name_code, data_dir)


def plot_vix_seasonality():
    path = os.path.join(data_dir, 'vix'+'.csv')
    df = pd.read_csv(path)
    t = pd.DatetimeIndex(pd.to_datetime(df['time'], format='%Y-%m-%d'))
    data = np.array(df['vix'])
    plot_daily_data_seasonality(t, data, 'vix seasonality', start_time='2010-01-01')


def plot_commodity_vix(variety):
    if variety == 'au':
        path = os.path.join(cfd_dir, 'GOLD_CFD'+'.csv')
        name = 'GOLD CFD'
        vix_name = 'gold_etf_vix'
    elif variety == 'sc':
        path = os.path.join(cfd_dir, 'WTI_CFD'+'.csv')
        name = 'WTI CFD'
        vix_name = 'oil_etf_vix'
    else:
        return
        
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    data = np.array(df1['close'], dtype=float)

    path = os.path.join(data_dir, 'vix'+'.csv') 
    df2 = pd.read_csv(path)
    t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    vix = np.array(df2[vix_name], dtype=float)

    datas = [[[[t1,data,name,'color=black'],],[[t2,vix,vix_name,'']],''],
             [[[t2,vix,vix_name,'color=blue'],],[],''],]
    plot_many_figure(datas, start_time='2015-01-01')


if __name__=="__main__":
    update_vix()

    # plot_vix_seasonality()




