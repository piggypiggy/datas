import os
import pandas as pd
import datetime
import numpy as np
from utils import *
from cftc import *
from msci import *
from vix import *


def plot_oil():
    path = os.path.join(data_dir, 'CFD', 'WTI_CFD'+'.csv') 
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    wti = np.array(df1['close'], dtype=float)

    cftc_plot_disaggregated(t1, wti, 'WTI CFD', code='067651', inst_name='NYMEX:WTI原油PHYSICAL')
    # cftc_plot_disaggregated(t1, wti, 'WTI CFD', code='06765A', inst_name='NYMEX:WTI原油FINANCIAL')

    plot_commodity_vix('sc')


def plot_gasoline():
    path = os.path.join(data_dir, 'CFD', 'GASOLINE_CFD'+'.csv') 
    df1 = pd.read_csv(path)
    t1 = pd.DatetimeIndex(pd.to_datetime(df1['time'], format='%Y-%m-%d'))
    wti = np.array(df1['close'], dtype=float)

    # path = os.path.join(data_dir, 'DIESEL OIL CFD'+'.csv') 
    # df2 = pd.read_csv(path)
    # t2 = pd.DatetimeIndex(pd.to_datetime(df2['time'], format='%Y-%m-%d'))
    # diesel = np.array(df2['close'], dtype=float)

    cftc_plot_disaggregated(t1, wti, 'GASOLINE CFD', code='111659', inst_name='NYMEX:GASOLINE RBOB')


if __name__=="__main__":

    plot_oil()
    plot_saudi_vs_oil()
    plot_gasoline()


    pass
